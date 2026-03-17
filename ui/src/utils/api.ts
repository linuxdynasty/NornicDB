// NornicDB API Client

import { BASE_PATH, joinBasePath } from './basePath';

export interface AuthConfig {
  devLoginEnabled: boolean;
  securityEnabled: boolean;
  oauthProviders: Array<{
    name: string;
    url: string;
    displayName: string;
  }>;
}

export interface DatabaseStats {
  status: string;
  server: {
    uptime_seconds: number;
    requests: number;
    errors: number;
    active: number;
  };
  database: {
    nodes: number;
    edges: number;
  };
}

export interface SearchResult {
  node: {
    id: string;
    labels: string[];
    properties: Record<string, unknown>;
    created_at: string;
  };
  score: number;
  rrf_score?: number;
  vector_rank?: number;
  bm25_rank?: number;
}

export interface CypherResponse {
  results: Array<{
    columns: string[];
    data: Array<{
      row: unknown[];
      meta: unknown[];
    }>;
  }>;
  errors?: Array<{
    code: string;
    message: string;
  }>;
}

export interface ConstituentInfo {
  alias: string;
  databaseName: string;
  type: string;       // "local" or "remote"
  accessMode: string; // "read", "write", "read_write"
  uri?: string;       // only for remote constituents
}

export interface DatabaseInfo {
  name: string;
  status: string;
  default: boolean;
  type?: string;                    // "standard", "composite", "system"
  constituents?: ConstituentInfo[]; // only for composite databases
  nodeCount: number;
  edgeCount: number;
  nodeStorageBytes?: number;
  managedEmbeddingBytes?: number;
  searchReady?: boolean;
  searchBuilding?: boolean;
  searchInitialized?: boolean;
  searchStrategy?: string;
  searchPhase?: string;
  searchProcessed?: number;
  searchTotal?: number;
  searchRate?: number;
  searchEtaSeconds?: number;
}

export interface DatabaseRow {
  [key: string]: unknown;
  name: string;
  type?: string;
  access?: string;
  role?: string;
  status?: string;
  default?: boolean;
}

interface DiscoveryResponse {
  bolt_direct: string;
  bolt_routing: string;
  transaction: string;
  neo4j_version: string;
  neo4j_edition: string;
  default_database?: string; // NornicDB extension
}

class NornicDBClient {
  private defaultDatabase: string | null = null;
  private static readonly TX_COMMIT_TIMEOUT_MS = 10 * 60 * 1000; // 10 minutes

  private async fetchWithTimeout(url: string, init: RequestInit, timeoutMs: number): Promise<Response> {
    const controller = new AbortController();
    const timeoutHandle = window.setTimeout(() => controller.abort(), timeoutMs);
    try {
      return await fetch(url, { ...init, signal: controller.signal });
    } catch (err) {
      if (err instanceof DOMException && err.name === 'AbortError') {
        throw new Error(`Request timed out after ${Math.floor(timeoutMs / 1000)} seconds`);
      }
      throw err;
    } finally {
      window.clearTimeout(timeoutHandle);
    }
  }

  private async parseErrorMessage(res: Response, fallback: string): Promise<string> {
    const raw = await res.text().catch(() => '');
    if (raw) {
      try {
        const payload = JSON.parse(raw) as { message?: string; error?: string };
        return payload?.message || payload?.error || raw || fallback;
      } catch {
        return raw;
      }
    }
    return fallback;
  }

  private async parseCypherResponseOrThrow(res: Response, fallback: string): Promise<CypherResponse> {
    if (!res.ok) {
      const message = await this.parseErrorMessage(res, `${fallback} (${res.status})`);
      throw new Error(message);
    }
    const raw = await res.text().catch(() => '');
    if (!raw) {
      throw new Error(`${fallback}: empty response`);
    }
    try {
      return JSON.parse(raw) as CypherResponse;
    } catch {
      throw new Error(`${fallback}: invalid JSON response`);
    }
  }

  private async postCypherCommit(
    dbName: string,
    statement: string,
    parameters?: Record<string, unknown>,
    timeoutMs: number = NornicDBClient.TX_COMMIT_TIMEOUT_MS,
  ): Promise<CypherResponse> {
    const res = await this.fetchWithTimeout(
      joinBasePath(BASE_PATH, `/db/${encodeURIComponent(dbName)}/tx/commit`),
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
          statements: [{ statement, parameters }],
        }),
      },
      timeoutMs,
    );
    return this.parseCypherResponseOrThrow(res, 'Cypher request failed');
  }

  // Get default database name from discovery endpoint
  private async getDefaultDatabase(): Promise<string> {
    // Return cached value if available
    if (this.defaultDatabase) {
      return this.defaultDatabase;
    }

    try {
      const res = await fetch(joinBasePath(BASE_PATH, '/'), { credentials: 'include' });
      if (res.ok) {
        const discovery: DiscoveryResponse = await res.json();
        // Cache the default database name
        this.defaultDatabase = discovery.default_database || 'nornic';
        return this.defaultDatabase;
      }
    } catch {
      // Fallback to default if discovery fails
    }

    // Fallback to NornicDB's default
    this.defaultDatabase = 'nornic';
    return this.defaultDatabase;
  }

  async getAuthConfig(): Promise<AuthConfig> {
    try {
      const res = await fetch(joinBasePath(BASE_PATH, '/auth/config'), { credentials: 'include' });
      if (res.ok) {
        return await res.json();
      }
      // Default config if endpoint doesn't exist
      return {
        devLoginEnabled: true,
        securityEnabled: false,
        oauthProviders: [],
      };
    } catch {
      // Auth disabled by default
      return {
        devLoginEnabled: true,
        securityEnabled: false,
        oauthProviders: [],
      };
    }
  }

  async checkAuth(): Promise<{ authenticated: boolean; user?: string }> {
    try {
      const res = await fetch(joinBasePath(BASE_PATH, '/auth/me'), { credentials: 'include' });
      if (res.ok) {
        const data = await res.json();
        return { authenticated: true, user: data.username };
      }
      return { authenticated: false };
    } catch {
      return { authenticated: false };
    }
  }

  async login(username: string, password: string): Promise<{ success: boolean; error?: string }> {
    try {
      const res = await fetch(joinBasePath(BASE_PATH, '/auth/token'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ username, password }),
      });
      
      if (res.ok) {
        return { success: true };
      }
      
      const data = await res.json().catch(() => ({ message: 'Login failed' }));
      return { success: false, error: data.message || 'Invalid credentials' };
    } catch {
      return { success: false, error: 'Network error' };
    }
  }

  async logout(): Promise<void> {
    await fetch(joinBasePath(BASE_PATH, '/auth/logout'), {
      method: 'POST',
      credentials: 'include',
    });
  }

  async getHealth(): Promise<{ status: string; time: string }> {
    const res = await fetch(joinBasePath(BASE_PATH, '/health'));
    return await res.json();
  }

  async getStatus(): Promise<DatabaseStats> {
    const res = await fetch(joinBasePath(BASE_PATH, '/status'));
    return await res.json();
  }

  async search(
    query: string,
    limit: number = 10,
    labels?: string[],
    database?: string
  ): Promise<SearchResult[]> {
    const body: { query: string; limit: number; labels?: string[]; database?: string } = {
      query,
      limit,
      labels,
    };
    if (database != null && database !== "") {
      body.database = database;
    }
    const res = await fetch(joinBasePath(BASE_PATH, '/nornicdb/search'), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'include',
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      if (res.status === 503) {
        throw new Error('Search is warming up. Please try again in a moment.');
      }
      const message = await this.parseErrorMessage(res, `Search failed (${res.status})`);
      throw new Error(message);
    }
    return await res.json();
  }

  async findSimilar(
    nodeId: string,
    limit: number = 10,
    database?: string
  ): Promise<SearchResult[]> {
    const body: { node_id: string; limit: number; database?: string } = {
      node_id: nodeId,
      limit,
    };
    if (database != null && database !== "") {
      body.database = database;
    }
    const res = await fetch(joinBasePath(BASE_PATH, '/nornicdb/similar'), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'include',
      body: JSON.stringify(body),
    });
    return await res.json();
  }

  async executeCypher(statement: string, parameters?: Record<string, unknown>, database?: string): Promise<CypherResponse> {
    const dbName = database != null && database !== '' ? database : await this.getDefaultDatabase();
    return this.postCypherCommit(dbName, statement, parameters);
  }

  async executeCypherOnDatabase(dbName: string, statement: string, parameters?: Record<string, unknown>): Promise<CypherResponse> {
    return this.postCypherCommit(dbName, statement, parameters);
  }

  async executeSystemCypher(statement: string, parameters?: Record<string, unknown>): Promise<CypherResponse> {
    return this.executeCypherOnDatabase('system', statement, parameters);
  }

  async getDatabaseInfo(name: string): Promise<DatabaseInfo> {
    const res = await fetch(joinBasePath(BASE_PATH, `/db/${encodeURIComponent(name)}`), {
      method: 'GET',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'include',
    });

    if (!res.ok) {
      const error = await res.json().catch(() => ({ message: 'Failed to get database info' }));
      throw new Error(error.message || 'Failed to get database info');
    }
    return await res.json();
  }

  private parseCypherRows<T extends Record<string, unknown>>(resp: CypherResponse): T[] {
    const result = resp.results?.[0];
    const columns = result?.columns || [];
    const data = result?.data || [];
    return data.map((d) => {
      const row = d.row || [];
      const out: Record<string, unknown> = {};
      for (let i = 0; i < columns.length; i++) {
        out[columns[i]] = row[i];
      }
      return out as T;
    });
  }

  async listDatabases(): Promise<DatabaseInfo[]> {
    const resp = await this.executeSystemCypher('SHOW DATABASES');
    if (resp.errors && resp.errors.length > 0) {
      throw new Error(resp.errors.map((e) => e.message).join('; '));
    }

    const rows = this.parseCypherRows<DatabaseRow>(resp);
    const names = rows
      .map((r) => (typeof r.name === 'string' ? r.name : ''))
      .filter((n) => n && n !== 'system');

    const infos = await Promise.all(
      names.map(async (name) => {
        try {
          return await this.getDatabaseInfo(name);
        } catch {
          return null;
        }
      }),
    );

    return infos.filter((x): x is DatabaseInfo => Boolean(x));
  }

  /** Returns database names for the query dropdown (user-visible DBs, excludes system). */
  async listDatabaseNames(): Promise<string[]> {
    const list = await this.listDatabases();
    return list.map((d) => d.name);
  }

  private quoteCypherIdentifier(identifier: string): string {
    // Cypher uses backticks for identifier quoting; escape embedded backticks by doubling them.
    // Example: db name `a`b` => `a``b`
    return `\`${identifier.split('`').join('``')}\``;
  }

  private validateDatabaseName(name: string): string {
    const trimmed = name.trim();
    if (!trimmed) {
      throw new Error('Database name is required');
    }
    if (trimmed.includes(':')) {
      throw new Error("Database name cannot include ':'");
    }
    if (trimmed.startsWith('_')) {
      throw new Error("Database name cannot start with '_'");
    }
    return trimmed;
  }

  async createDatabase(name: string): Promise<void> {
    const dbName = this.validateDatabaseName(name);
    const resp = await this.executeSystemCypher(`CREATE DATABASE ${this.quoteCypherIdentifier(dbName)}`);
    if (resp.errors && resp.errors.length > 0) {
      throw new Error(resp.errors.map((e) => e.message).join('; '));
    }
  }

  async dropDatabase(name: string): Promise<void> {
    const dbName = this.validateDatabaseName(name);
    const resp = await this.executeSystemCypher(`DROP DATABASE ${this.quoteCypherIdentifier(dbName)}`);
    if (resp.errors && resp.errors.length > 0) {
      throw new Error(resp.errors.map((e) => e.message).join('; '));
    }
  }

  async deleteNodes(nodeIds: string[], database?: string): Promise<{ success: boolean; deleted: number; errors: string[] }> {
    if (nodeIds.length === 0) {
      return { success: true, deleted: 0, errors: [] };
    }

    const dbName = database != null && database !== '' ? database : await this.getDefaultDatabase();
    
    try {
      // First, verify the nodes exist before deleting (safety check)
      const verifyStatement = `MATCH (n) WHERE id(n) IN $ids RETURN id(n) as nodeId, elementId(n) as elementId`;
      const verifyResult = await this.postCypherCommit(dbName, verifyStatement, { ids: nodeIds });
      const foundCount = verifyResult.results[0]?.data?.length || 0;
      
      if (foundCount === 0) {
        return {
          success: false,
          deleted: 0,
          errors: [
            `None of the requested nodes were found. ` +
            `Requested IDs: ${nodeIds.join(', ')}. ` +
            `This may indicate the nodes were already deleted or the IDs are incorrect.`
          ],
        };
      }
      
      if (foundCount !== nodeIds.length) {
        return {
          success: false,
          deleted: 0,
          errors: [
            `Only ${foundCount} of ${nodeIds.length} requested nodes were found. ` +
            `Requested IDs: ${nodeIds.join(', ')}. ` +
            `Some nodes may not exist.`
          ],
        };
      }
      
      // Use bulk delete with id(n) IN $ids - verified by unit tests to work correctly
      // This is much more efficient than deleting one by one
      // The UI extracts internal IDs from elementId, which id(n) matches perfectly
      const statement = `MATCH (n) WHERE id(n) IN $ids DETACH DELETE n RETURN count(n) as deleted`;
      const parameters = { ids: nodeIds };

      const result = await this.postCypherCommit(dbName, statement, parameters);
      
      if (result.errors && result.errors.length > 0) {
        return {
          success: false,
          deleted: 0,
          errors: result.errors.map(e => e.message),
        };
      }

      const deleted = result.results[0]?.data[0]?.row[0] as number || 0;
      
      // CRITICAL: If more nodes were deleted than requested, this is a serious bug
      // The WHERE clause should have filtered correctly - this indicates a query issue
      if (deleted > nodeIds.length) {
        return {
          success: false,
          deleted,
          errors: [
            `CRITICAL: Expected to delete ${nodeIds.length} nodes, but ${deleted} were deleted. ` +
            `This indicates the WHERE clause did not filter correctly. ` +
            `Requested IDs: ${nodeIds.join(', ')}`
          ],
        };
      }
      
      // If fewer nodes were deleted, some may not exist
      if (deleted < nodeIds.length) {
        return {
          success: false,
          deleted,
          errors: [
            `Expected to delete ${nodeIds.length} nodes, but only ${deleted} were deleted. ` +
            `Some nodes may not exist. Requested IDs: ${nodeIds.join(', ')}`
          ],
        };
      }

      return {
        success: true,
        deleted,
        errors: [],
      };
    } catch (err) {
      return {
        success: false,
        deleted: 0,
        errors: [err instanceof Error ? err.message : 'Unknown error'],
      };
    }
  }

  async updateNodeProperties(nodeId: string, properties: Record<string, unknown>, database?: string): Promise<{ success: boolean; error?: string }> {
    const dbName = database != null && database !== '' ? database : await this.getDefaultDatabase();
    
    // Build SET clause
    const setParts: string[] = [];
    const parameters: Record<string, unknown> = { nodeId };
    let paramIndex = 0;
    
    for (const [key, value] of Object.entries(properties)) {
      const paramName = `p${paramIndex}`;
      setParts.push(`n.${key} = $${paramName}`);
      parameters[paramName] = value;
      paramIndex++;
    }

    if (setParts.length === 0) {
      return { success: true };
    }

    const statement = `MATCH (n) WHERE id(n) = $nodeId OR n.id = $nodeId SET ${setParts.join(', ')} RETURN n`;
    
    try {
      const result = await this.postCypherCommit(dbName, statement, parameters);
      
      if (result.errors && result.errors.length > 0) {
        return {
          success: false,
          error: result.errors.map(e => e.message).join('; '),
        };
      }

      return { success: true };
    } catch (err) {
      return {
        success: false,
        error: err instanceof Error ? err.message : 'Failed to update node',
      };
    }
  }

  /** Per-database config: overrides and effective (admin only). */
  async getDatabaseConfig(dbName: string): Promise<{ overrides: Record<string, string>; effective: Record<string, string> }> {
    const res = await fetch(joinBasePath(BASE_PATH, `/admin/databases/${encodeURIComponent(dbName)}/config`), { credentials: 'include' });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.message ?? `Failed to load config: ${res.status}`);
    }
    return res.json();
  }

  /** Save per-database config overrides (admin only). */
  async putDatabaseConfig(dbName: string, overrides: Record<string, string>): Promise<{ overrides: Record<string, string>; rebuildTriggered?: boolean }> {
    const res = await fetch(joinBasePath(BASE_PATH, `/admin/databases/${encodeURIComponent(dbName)}/config`), {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'include',
      body: JSON.stringify({ overrides }),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.message ?? `Failed to save config: ${res.status}`);
    }
    return res.json();
  }

  /** Allowed per-DB config keys with type and category (admin only). */
  async getDatabaseConfigKeys(): Promise<Array<{ key: string; type: string; category: string }>> {
    const res = await fetch(joinBasePath(BASE_PATH, '/admin/databases/config/keys'), { credentials: 'include' });
    if (!res.ok) throw new Error('Failed to load config keys');
    return res.json();
  }
}

export const api = new NornicDBClient();
