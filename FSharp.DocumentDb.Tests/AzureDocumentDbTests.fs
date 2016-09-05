namespace FSharp.DocumentDb.Wrappers.Tests

open Microsoft.VisualStudio.TestTools.UnitTesting
open FSharp.DocumentDb.Wrappers.Azure
open FSharp.DocumentDb.Wrappers.Azure.ConnectionHelper
open FSharp.DocumentDb.Wrappers.Azure.DatabaseManagement
open FSharp.DocumentDb.Wrappers.Azure.QueryHelper
open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.Client
open Microsoft.Azure.Documents.Linq
open System.Linq

// Testing types
type Instrument(issue : int, issueName : string, serie : int, serieName : string, shares : int) = 
    member val Issue = issue with get, set
    member val IssueName = issueName with get, set
    member val Serie = serie with get, set
    member val SerieName = serieName with get, set
    member val Shares = shares with get, set

type PortfolioModel(instruments : list<Instrument>, portfolioModelId : int, name : string, categoryId : int, minInterval : int, maxInterval : int) = 
    member val Instruments = instruments with get, set
    member val Name = name with get, set
    member val CategoryId = categoryId with get, set
    member val Interval = (minInterval, maxInterval) with get, set
    member val PortfolioModelId = portfolioModelId with get, set
    member val id = "" with get, set

[<TestClass>]
type DocumentDBTest() = 
    let endpoint = "Your_URI_Here"
    let primaryKey = "Your_Key_Here" 
    let databaseName = "Investify"
    let collectionName = "mikeTestCollection"
    let instruments = [ new Instrument(1, "GBMCRE", 1, "BF", 100) ]
    let portfolioModel = new PortfolioModel(instruments, 1, "Deuda", 1, 1, 25)

    [<TestMethod>]
    member this.CheckDb() = 
        let azureConnection = new AzureDocumentDb(endpoint, primaryKey)
        let client = GetDatabaseClient(azureConnection)
        // Create Test Collection
        let newCollection = CreateCollection(databaseName, collectionName, client, System.Nullable<int> 0)
        // Insert Doc in new Collection 
        let loadedDoc = LoadDocument(client, databaseName, collectionName, portfolioModel)
        let getQueryable = GetIQueryable<PortfolioModel>(client, databaseName, collectionName)
        let select = getQueryable.ToList().FirstOrDefault()
        let newPortfolioModel = select
        newPortfolioModel.Name <- "Ya cambió"
        let replaceDoc = ReplaceDocument(client, databaseName, collectionName, newPortfolioModel)
        let deleteDoc = 
            DeleteDocument(client, databaseName, collectionName, newPortfolioModel.id.ToString())
        let sqlSelect = ExecuteSQL<PortfolioModel>(client, databaseName, collectionName, "SELECT * FROM PortfolioModel")
        // Nothing failed!
        Assert.IsTrue(true)

