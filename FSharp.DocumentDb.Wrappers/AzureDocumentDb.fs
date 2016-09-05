namespace FSharp.DocumentDb.Wrappers.Azure

module ConnectionHelper = 
    open System
    open System.Linq
    open System.Threading.Tasks

    open System.Net
    open Microsoft.Azure.Documents
    open Microsoft.Azure.Documents.Client
    open Newtonsoft.Json

    type AzureDocumentDb (endpointUri: string, primaryKey: string) =
        member this.EndpointUri = endpointUri
        member this.PrimaryKey = primaryKey

    let GetDatabaseClient(azureDocumentDb: AzureDocumentDb) =
        let uri = new Uri(azureDocumentDb.EndpointUri)
        let primaryKey =  azureDocumentDb.PrimaryKey
        let client = new DocumentClient(uri, primaryKey)
        client

module private ExceptionHandler =
    open System
    open Microsoft.Azure.Documents
    open Microsoft.Azure.Documents.Client
    open System.Net

    let CreateCollection (x:DocumentClientException, client:DocumentClient, databaseName:string, collectionName:string, requestOffer: System.Nullable<int>) = 
        match(x.StatusCode.Value.Equals(HttpStatusCode.NotFound)) with
        | true -> 
            let collectionInfo = new DocumentCollection()
            collectionInfo.Id <- collectionName

            //Recommendation from MSFT
            let rangeIndex = new RangeIndex(DataType.String)
            rangeIndex.Precision <-  System.Nullable<int16> -1s
            let ip = new IndexingPolicy(rangeIndex)
            collectionInfo.IndexingPolicy <- ip

            let dbUri = UriFactory.CreateDatabaseUri(databaseName)
            let requestOptions = new RequestOptions()
            if(requestOffer = new System.Nullable<int>() || requestOffer.Value < 400) 
                then requestOptions.OfferThroughput <- System.Nullable<int> 400 
                else requestOptions.OfferThroughput <- requestOffer
        
            let task = client.CreateDocumentCollectionAsync(dbUri, collectionInfo, requestOptions)
            let res = Async.AwaitTask(task) |> Async.RunSynchronously
            res
        | false -> failwith("Error creating Document Collection. Status Code: "+(x.StatusCode.ToString()))

    let CreateDocument(x:DocumentClientException, client:DocumentClient, databaseName:string, collectionName:string, obj) = 
        match x.StatusCode.Value with
            | HttpStatusCode.NotFound | HttpStatusCode.BadRequest ->
                let uri = UriFactory.CreateDocumentCollectionUri(databaseName, collectionName)
                Async.AwaitTask(client.CreateDocumentAsync(uri, obj)) |> Async.RunSynchronously
            | _ -> failwith("Error creating Document. Status Code: "+(x.StatusCode.ToString()))
                    
module DatabaseManagement = 
    open System
    open System.Linq
    open System.Threading.Tasks

    open System.Net
    open Microsoft.Azure.Documents
    open Microsoft.Azure.Documents.Client
    open Newtonsoft.Json

    let CreateDatabase(client: DocumentClient, databaseName: string) = 
        try
            let dbUri = UriFactory.CreateDatabaseUri(databaseName)
            Async.AwaitTask(client.ReadDatabaseAsync(dbUri)) |> Async.RunSynchronously
        with
            | :? DocumentClientException as ex -> 
                if(ex.StatusCode.Value.Equals(HttpStatusCode.NotFound)) 
                then 
                    let db = new Database() 
                    db.Id <- databaseName
                    Async.AwaitTask(client.CreateDatabaseAsync(db)) |> Async.RunSynchronously
                else
                    raise ex
    
    // when shit hits the fan...
    let DeleteDatabase (client: DocumentClient, databaseName: string) =
        let uri = UriFactory.CreateDatabaseUri(databaseName)
        Async.AwaitTask(client.DeleteDatabaseAsync(uri)) |> Async.RunSynchronously
        
    let CreateCollection(databaseName: string, collectionName: string, client: DocumentClient, requestOffer: System.Nullable<int>) =
        let collection = UriFactory.CreateDocumentCollectionUri(databaseName, collectionName)
        try
            let task = client.ReadDocumentCollectionAsync(collection)
            let res = Async.AwaitTask(task) |> Async.RunSynchronously    
            res
        with
            | :? AggregateException as ex ->
                let mutable result = null
                ex.Handle(fun x ->
                    match x with
                        | :? DocumentClientException as dce ->  
                            let res = ExceptionHandler.CreateCollection(dce, client, databaseName, collectionName, requestOffer)
                            result <- res
                            true
                        | _ -> raise x
                    )
                result

module QueryHelper =
    open System
    open System.Linq
    open System.Threading.Tasks

    open System.Net
    open Microsoft.Azure.Documents
    open Microsoft.Azure.Documents.Client
    open Newtonsoft.Json
    open System.Collections.Generic

    let private queryOptions =
        let feedOptions = new FeedOptions()
        feedOptions.MaxItemCount <- System.Nullable<int> -1
        feedOptions
    
    let private tryGetIdValue(obj) =
        let mutable res = ""
        // Default name by Azure
        let id = obj.GetType().GetProperty("id").GetValue(obj, null)
        match id with
            | null -> ()
            | _ -> res <- id.ToString()
        res

    let LoadDocument (client:DocumentClient, databaseName: string, collectionName: string, obj) = 
        try
            let id = tryGetIdValue(obj)
            let documentUri = UriFactory.CreateDocumentUri(databaseName, collectionName, id)
            Async.AwaitTask(client.ReadDocumentAsync(documentUri)) |> Async.RunSynchronously 
        with
            | :? AggregateException as ex ->
                let mutable result = null
                ex.Handle(fun x ->
                    match x with
                        | :? DocumentClientException as dce ->  
                            let res = ExceptionHandler.CreateDocument(dce, client, databaseName, collectionName, obj)
                            result <- res
                            true
                        | _ -> raise x
                    )
                result

    let GetIQueryable<'T> (client: DocumentClient, databaseName:string, collectionName: string) = 
        let documentCollection = UriFactory.CreateDocumentCollectionUri(databaseName, collectionName)
        let iQueryable = client.CreateDocumentQuery<'T>(documentCollection, queryOptions)
        iQueryable

    let ExecuteSQL<'T> (client: DocumentClient, databaseName:string, collectionName: string, sqlQuery: string) = 
        let documentCollection = UriFactory.CreateDocumentCollectionUri(databaseName, collectionName)
        let iQueryable = client.CreateDocumentQuery<'T>(documentCollection, sqlQuery, queryOptions)
        iQueryable
      
    let ReplaceDocument (client:DocumentClient, databaseName: string, collectionName: string, obj) = 
        try
            let id = tryGetIdValue(obj)
            let documentUri = UriFactory.CreateDocumentUri(databaseName, collectionName, id)
            Async.AwaitTask(client.ReplaceDocumentAsync(documentUri, obj)) |> Async.RunSynchronously
        with
            | :? AggregateException as ex -> 
                ex.Handle(fun x -> 
                    match x with 
                        | :? DocumentClientException as dce -> raise dce
                        | _ -> raise x
                )
                raise ex

    let DeleteDocument (client:DocumentClient, databaseName: string, collectionName: string, idValue: string) = 
        try
            let documentUri = UriFactory.CreateDocumentUri(databaseName, collectionName, idValue)
            Async.AwaitTask(client.DeleteDocumentAsync(documentUri)) |> Async.RunSynchronously
        with  
            | :? AggregateException as ex -> 
                ex.Handle(fun x -> 
                    match x with 
                        | :? DocumentClientException as dce -> raise dce
                        | _ -> raise x
                )
                raise ex
        
                         
                           
