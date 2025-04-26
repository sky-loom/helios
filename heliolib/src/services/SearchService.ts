import { ContextService } from "./ContextService.js";
import fs from "fs";
import flexsearch, { Index as FlexSearchIndex, Resolver } from "flexsearch";
import { AppBskyFeedDefs, AppBskyFeedPost } from "@atproto/api";
import { EmbeddingCreateParams } from "openai/src/resources/embeddings.js";
import { ChromaClient } from "chromadb";
import OpenAI from "openai";
import { getDIDFromATUri } from "../utils/bskyutils.js"; // Adjust the import path as necessary
const { Index } = flexsearch;

export class SearchService {
  private contexts: ContextService;
  private loadedSearchIndex: string = "";
  private llmclient?: OpenAI;
  private chromaclient = new ChromaClient();

  constructor(context: ContextService) {
    this.contexts = context;
    try {
      this.llmclient = new OpenAI({
        baseURL: "http://localhost:1234/v1",
        apiKey: "",
      });
    } catch (err) {
      this.llmclient = undefined;
      console.log("Whoops, no LLM running");
    }
    this.chromaclient = new ChromaClient();
  }
  async ExportFlexSearchIndex(filename: string) {
    const context = await this.contexts.GetContext(this.contexts.selectedContext);
    if (context) {
      if (!fs.existsSync(`./searchdata/${filename}/`)) {
        fs.mkdirSync(`./searchdata/${filename}/`, { recursive: true });
        console.log(`Directory created: ./searchdata/${filename}/`);
      }
    }
    await context?.index.export((key, data) => {
      console.log("writing...." + key);
      fs.writeFileSync(`./searchdata/${filename}/${key}.json`, data);
    });
  }
  async ImportFlexSearchIndex(filename: string) {
    console.log("Loading Search Index");
    if (this.loadedSearchIndex != filename) {
      this.loadedSearchIndex = filename;
      const context = await this.contexts.GetContext(this.contexts.selectedContext);
      if (context) {
        const keys = fs
          .readdirSync(`./searchdata/${filename}/`, { withFileTypes: true })
          .filter((item) => !item.isDirectory())
          .map((item) => item.name);

        for (let i = 0, key; i < keys.length; i += 1) {
          console.log(keys[i]);
          key = keys[i];
          const data = fs.readFileSync(`./searchdata/${filename}/${key}`, "utf8");
          await context.index.import(key.replace(".json", ""), data);
        }
      }
    }
    //
    console.log("loaded!");
  }
  private async AddToChroma(did: string, id: string, text: string, overwrite: boolean) {
    //get embeddings

    let safedid = did.replace("did:plc:", "did-plc-").replace("did:web:", "did-web-");
    console.log("Adding to Chroma: " + id);
    const collection = await this.chromaclient.getOrCreateCollection({
      name: safedid,
    });

    let idresult =
      (
        await collection.get({
          ids: [id],
        })
      ).ids.length == 0;

    if (overwrite || idresult == false) {
      console.log("Overwrite or ID not found - writing");
      let req: EmbeddingCreateParams = {
        input: text,
        model: "text-embedding-nomic-embed-text-v1.5-embedding",
        encoding_format: "float",
      };
      console.log(req);
      const vectors = await this.llmclient?.embeddings.create(req);
      //chroma
      if (vectors) {
        var embeddings = (vectors.data[0] as OpenAI.Embedding)?.embedding;
        const response = await collection.upsert({
          ids: [id],
          embeddings: [embeddings],
          metadatas: [{}],
          documents: [""],
        });
      }
    }
  }
  private async AddToTextSearch(did: string, id: string, text: string, overwrite: boolean) {
    console.log("Adding to FlexSearch: " + id);
    let context = await this.contexts.GetContext(this.contexts.selectedContext);
    let data = context?.posts;
    if (context && data) {
      for (const entry of context.posts) {
        if (overwrite || !context.index.contain(id)) context.index.add(id, text);
      }
    }
  }

  async EmbedAndIndexContext(options: { flexsearch: boolean; chroma: boolean; overwrite: boolean }) {
    const context = await this.contexts.GetContext(this.contexts.selectedContext);
    if (context) {
      let at = 0;
      for (const entry of context.posts) {
        at++;
        console.log("Indexing .... " + at + "/" + context.posts.size);
        const did = getDIDFromATUri(entry[1].uri);
        if (did) {
          //console.log(entry[1]);
          if (options.chroma)
            await this.AddToChroma(did, entry[1].uri, (entry[1].record as AppBskyFeedPost.Record).text, options.overwrite);
          if (options.flexsearch)
            this.AddToTextSearch(did, entry[1].uri, (entry[1].record as AppBskyFeedPost.Record).text, options.overwrite);
        }
        //if (at % 100 == 0) break;
      }
      if (options.flexsearch) await this.ExportFlexSearchIndex(context.name);
    }
  }

  async Search(did: string, text: string, searchtype: string): Promise<AppBskyFeedDefs.PostView[]> {
    let results: AppBskyFeedDefs.PostView[] = [];
    if (searchtype == "text") {
      results = await this.TextSearch(text);
    } else if (searchtype == "semantic") {
      results = await this.SemanticSearch(did, text);
    } else {
      results = await this.TextSearch(text);
      results = [...results, ...(await this.SemanticSearch(did, text))];
    }
    return results;
  }

  async TextSearch(text: string): Promise<AppBskyFeedDefs.PostView[]> {
    let context = await this.contexts.GetContext(this.contexts.selectedContext);
    let results: AppBskyFeedDefs.PostView[] = [];
    //console.log("searching..." + text);
    //console.log(JSON.stringify(context?.index, null, 2));
    if (text == "" && context) {
      context.resultset = context?.posts;
      results = context.resultset.values().toArray();
    } else {
      var txtResults = await context?.index.search(text, 10000);
      if (txtResults != undefined && context != undefined) {
        context.resultset = new Map();
        //console.log(txtResults);
        (txtResults as Resolver).result.forEach((res) => {
          //for (const res of txtResults) {
          //txtResults.forEach((res) => {
          //get each entry
          console.log("[" + res.toString() + "]");
          var record = context.posts.get(res.toString());
          //console.log(record);
          if (record != undefined) {
            results.push(record);
            context.resultset.set(res.toString(), record);
          }
          //console.log(record);
        });
        //);
        //console.log(context.resultset);
      }
    }

    return results;
  }

  async SemanticSearch(did: string, text: string) {
    let safedid = did.replace("did:plc:", "did-plc-").replace("did:web:", "did-web-");
    let context = await this.contexts.GetContext(this.contexts.selectedContext);
    let results: AppBskyFeedDefs.PostView[] = [];
    let req: EmbeddingCreateParams = {
      input: text,
      model: "text-embedding-nomic-embed-text-v1.5-embedding",
      encoding_format: "float",
    };
    console.log(req);
    const vectors = await this.llmclient?.embeddings.create(req);
    if (vectors) {
      //chroma
      const collection = await this.chromaclient.getOrCreateCollection({
        name: safedid,
      });
      var embeddings = (vectors.data[0] as OpenAI.Embedding)?.embedding;

      const queryresults = await collection.query({
        queryEmbeddings: [embeddings],
        nResults: 10,
      });
      if (context) context.resultset = new Map();
      queryresults.ids.forEach((ids) => {
        console.log(ids);
        for (const id of ids) {
          console.log(ids);
          var record = context?.posts.get(id.toString());
          if (record) {
            results.push(record);
            context?.resultset.set(id.toString(), record);
          }
        }
      });
    }
    return results;
  }
}
