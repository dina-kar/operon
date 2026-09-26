import type { Schemas } from '../api/client';

/** Every action a policy or a key can grant, in the contract's order. */
export const ACTIONS: { id: Schemas['Action']; label: string }[] = [
  { id: 'query', label: 'Query (search and SQL)' },
  { id: 'collections:read', label: 'Read collections and documents' },
  { id: 'collections:write', label: 'Write documents' },
  { id: 'documents:delete', label: 'Delete documents' },
  { id: 'streams:read', label: 'Read streams' },
  { id: 'streams:produce', label: 'Produce to streams' },
  { id: 'mcp:tools', label: 'Use MCP tools' },
];
