import couchbase, { type Cluster, type Collection } from "couchbase";
import { config } from "../config.ts";

export type CouchbaseHandles = {
  cluster: Cluster;
  history: Collection;
  state: Collection;
  close: () => Promise<void>;
};

export async function connectCouchbase(): Promise<CouchbaseHandles> {
  const cluster = await couchbase.connect(config.couchbase.connStr, {
    username: config.couchbase.username,
    password: config.couchbase.password,
  });
  const bucket = cluster.bucket(config.couchbase.bucket);
  const scope = bucket.scope(config.couchbase.scope);
  const history = scope.collection(config.couchbase.historyCollection);
  const state = scope.collection(config.couchbase.stateCollection);
  return {
    cluster,
    history,
    state,
    async close() {
      await cluster.close();
    },
  };
}
