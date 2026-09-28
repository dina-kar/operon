| Group | Fault | Attempt 1 | Attempt 2 |
|---|---|---|---|
| create_namespace | Refuse (BeforeSend) | Retried | Retried |
| create_namespace | LoseAck (AfterApply) | SurfacedUnknown | SurfacedUnknown |
| create_namespace | Undetermined | Retried | Retried |
| create_namespace | Conflict | Retried | Retried |
| create_namespace | Delay(500ms) | Retried | Retried |
| create_namespace | Race | Rejected | Rejected |
| create_collection | Refuse (BeforeSend) | Retried | Retried |
| create_collection | LoseAck (AfterApply) | Retried | Retried |
| create_collection | Undetermined | Retried | Retried |
| create_collection | Conflict | Retried | Retried |
| create_collection | Delay(500ms) | Retried | Retried |
| create_collection | Race | Rejected | Rejected |
| commit_wal | Refuse (BeforeSend) | Retried | Retried |
| commit_wal | LoseAck (AfterApply) | SurfacedUnknown | SurfacedUnknown |
| commit_wal | Undetermined | Retried | Retried |
| commit_wal | Conflict | Retried | Retried |
| commit_wal | Delay(500ms) | Retried | Retried |
| commit_wal | Race | Retried | Retried |
| commit_wal (2 groups, fault in group 1) | Refuse (BeforeSend) | Retried | Retried |
| commit_wal (2 groups, fault in group 1) | LoseAck (AfterApply) | SurfacedUnknown | SurfacedUnknown |
| commit_wal (2 groups, fault in group 1) | Undetermined | Retried | Retried |
| commit_wal (2 groups, fault in group 1) | Conflict | Retried | Retried |
| commit_wal (2 groups, fault in group 1) | Delay(500ms) | Retried | Retried |
| commit_wal (2 groups, fault in group 1) | Race | Retried | Retried |
| commit_wal (2 groups, fault in group 2) | Refuse (BeforeSend) | Retried | Retried |
| commit_wal (2 groups, fault in group 2) | LoseAck (AfterApply) | SurfacedUnknown | SurfacedUnknown |
| commit_wal (2 groups, fault in group 2) | Undetermined | Retried | Retried |
| commit_wal (2 groups, fault in group 2) | Conflict | Retried | Retried |
| commit_wal (2 groups, fault in group 2) | Delay(500ms) | Retried | Retried |
| commit_wal (2 groups, fault in group 2) | Race | Retried | Retried |
| swap_segment | Refuse (BeforeSend) | Retried | Retried |
| swap_segment | LoseAck (AfterApply) | SurfacedUnknown | SurfacedUnknown |
| swap_segment | Undetermined | Retried | Retried |
| swap_segment | Conflict | Retried | Retried |
| swap_segment | Delay(500ms) | Retried | Retried |
| swap_segment | Race | Retried | Retried |
| trim_partition | Refuse (BeforeSend) | Retried | Retried |
| trim_partition | LoseAck (AfterApply) | Retried | Retried |
| trim_partition | Undetermined | Retried | Retried |
| trim_partition | Conflict | Retried | Retried |
| trim_partition | Delay(500ms) | Retried | Retried |
| trim_partition | Race | Retried | Retried |
| cas_pointer | Refuse (BeforeSend) | Retried | Retried |
| cas_pointer | LoseAck (AfterApply) | SurfacedUnknown | SurfacedUnknown |
| cas_pointer | Undetermined | Retried | Retried |
| cas_pointer | Conflict | Retried | Retried |
| cas_pointer | Delay(500ms) | Retried | Retried |
| cas_pointer | Race | Rejected | Rejected |
| cas_pointer (fenced) | Refuse (BeforeSend) | Retried | Retried |
| cas_pointer (fenced) | LoseAck (AfterApply) | SurfacedUnknown | SurfacedUnknown |
| cas_pointer (fenced) | Undetermined | Retried | Retried |
| cas_pointer (fenced) | Conflict | Retried | Retried |
| cas_pointer (fenced) | Delay(500ms) | Retried | Retried |
| cas_pointer (fenced) | Race | Retried | Retried |
| acquire_lease | Refuse (BeforeSend) | Retried | Retried |
| acquire_lease | LoseAck (AfterApply) | Retried | Retried |
| acquire_lease | Undetermined | Retried | Retried |
| acquire_lease | Conflict | Retried | Retried |
| acquire_lease | Delay(500ms) | Retried | Retried |
| acquire_lease | Race | Rejected | Rejected |
| drop_collection | Refuse (BeforeSend) | Retried | Retried |
| drop_collection | LoseAck (AfterApply) | SurfacedUnknown | SurfacedUnknown |
| drop_collection | Undetermined | Retried | Retried |
| drop_collection | Conflict | Retried | Retried |
| drop_collection | Delay(500ms) | Retried | Retried |
| drop_collection | Race | Retried | Retried |
| drop_collection (64 partitions, 1 024 entries) | Refuse (BeforeSend) | Retried | Retried |
| drop_collection (64 partitions, 1 024 entries) | LoseAck (AfterApply) | SurfacedUnknown | SurfacedUnknown |
| drop_collection (64 partitions, 1 024 entries) | Undetermined | Retried | Retried |
| drop_collection (64 partitions, 1 024 entries) | Conflict | Retried | Retried |
| drop_collection (64 partitions, 1 024 entries) | Delay(500ms) | Retried | Retried |
| drop_collection (64 partitions, 1 024 entries) | Race | Retried | Retried |

## Race competitors

| Group | Attempt 1 | Attempt 2 |
|---|---|---|
| create_namespace | committed | committed |
| create_collection | committed | committed |
| commit_wal | committed after a restart | committed after a restart |
| commit_wal (2 groups, fault in group 1) | committed | committed |
| commit_wal (2 groups, fault in group 2) | committed after a restart | committed after a restart |
| swap_segment | committed after a restart | committed after a restart |
| trim_partition | committed after a restart | committed after a restart |
| cas_pointer | committed | committed |
| cas_pointer (fenced) | committed | committed |
| acquire_lease | committed | committed |
| drop_collection | committed | committed |
| drop_collection (64 partitions, 1 024 entries) | committed | committed |
