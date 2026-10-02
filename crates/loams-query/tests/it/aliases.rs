//! Aliases with several members through `CollectionService` (M1.5 Task 0a
//! rule 8): `update_alias_targets`, `resolve_name`, `list_aliases`, the
//! resolve step of single-collection methods, `CollectionInfo.aliases` and
//! the SQL table names.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::common::{Fixture, WAIT, tail_schema, upsert};
use loams_collection::PrimaryKey;
use loams_query::{
    AliasAction, AliasInfo, AliasMember, AliasTargetAction, CollectionService, NameInfo,
    Projection, ReadConsistency, SearchRequest, ServiceConfig, ServiceError, WriteOptions,
};
use serde_json::json;

const NS: &str = "acme";

fn add(alias: &str, collection: &str, is_write_index: Option<bool>) -> AliasTargetAction {
    AliasTargetAction::Add {
        alias: alias.to_string(),
        collection: collection.to_string(),
        is_write_index,
    }
}

fn member(collection: &str, is_write_index: Option<bool>) -> AliasMember {
    AliasMember {
        collection: collection.to_string(),
        is_write_index,
    }
}

fn not_found(name: &str) -> ServiceError {
    ServiceError::NotFound {
        kind: "collection",
        name: name.to_string(),
    }
}

/// A fixture with collections `names` in namespace `acme`.
async fn with_collections(names: &[&str]) -> (Fixture, Arc<CollectionService>) {
    let f = Fixture::start_with(ServiceConfig::default()).await;
    let service = f.service();
    for name in names {
        service
            .create_collection(NS, name, tail_schema(), None)
            .await
            .expect("create collection");
    }
    (f, service)
}

#[tokio::test]
async fn resolve_name_reports_members_and_the_write_target() {
    let (f, service) = with_collections(&["test_index2", "test_index1"]).await;
    service
        .update_alias_targets(
            NS,
            vec![
                add("test_alias", "test_index1", None),
                add("test_alias", "test_index2", Some(true)),
            ],
        )
        .await
        .expect("alias");
    assert_eq!(
        service.resolve_name(NS, "test_alias").await,
        Ok(NameInfo::Alias(AliasInfo {
            alias: "test_alias".to_string(),
            members: vec![
                member("test_index1", None),
                member("test_index2", Some(true)),
            ],
            write_target: Some("test_index2".to_string()),
        }))
    );
    assert_eq!(
        service.resolve_name(NS, "test_index1").await,
        Ok(NameInfo::Collection("test_index1".to_string()))
    );
    assert_eq!(
        service.resolve_name(NS, "nope").await,
        Err(not_found("nope"))
    );
    assert_eq!(
        service.resolve_name("nowhere", "test_alias").await,
        Err(not_found("test_alias"))
    );
    // Setting the write index to false leaves none.
    service
        .update_alias_targets(NS, vec![add("test_alias", "test_index2", Some(false))])
        .await
        .expect("unset");
    match service.resolve_name(NS, "test_alias").await {
        Ok(NameInfo::Alias(info)) => assert_eq!(info.write_target, None),
        other => panic!("expected an alias, got {other:?}"),
    }
    f.shutdown().await;
}

#[tokio::test]
async fn list_aliases_lists_both_kinds_by_name() {
    let (f, service) = with_collections(&["a", "b"]).await;
    service
        .update_aliases(
            NS,
            vec![AliasAction::Create {
                alias: "zz-single".to_string(),
                collection: "b".to_string(),
            }],
        )
        .await
        .expect("single-target alias");
    service
        .update_alias_targets(NS, vec![add("multi", "b", None), add("multi", "a", None)])
        .await
        .expect("multi-target alias");
    assert_eq!(
        service.list_aliases(NS).await,
        Ok(vec![
            AliasInfo {
                alias: "multi".to_string(),
                members: vec![member("a", None), member("b", None)],
                write_target: None,
            },
            AliasInfo {
                alias: "zz-single".to_string(),
                members: vec![member("b", None)],
                write_target: Some("b".to_string()),
            },
        ])
    );
    assert_eq!(service.list_aliases("nowhere").await, Ok(Vec::new()));
    f.shutdown().await;
}

#[tokio::test]
async fn single_collection_operations_on_a_multi_target_alias_are_invalid_argument() {
    let (f, service) = with_collections(&["b", "a"]).await;
    service
        .update_alias_targets(NS, vec![add("al", "b", Some(true)), add("al", "a", None)])
        .await
        .expect("alias");
    let refused = || {
        Err::<(), _>(ServiceError::InvalidArgument(
            "alias [al] names 2 collections [a, b]; this operation needs one collection"
                .to_string(),
        ))
    };
    assert_eq!(
        service.get_collection(NS, "al").await.map(|_| ()),
        refused()
    );
    assert_eq!(
        service
            .write(
                NS,
                "al",
                vec![upsert(1, json!({"t": "x"}))],
                WriteOptions::default()
            )
            .await
            .map(|_| ()),
        refused()
    );
    assert_eq!(
        service
            .search(NS, SearchRequest::new("al"))
            .await
            .map(|_| ()),
        refused()
    );
    assert_eq!(
        service
            .count(NS, "al", None, ReadConsistency::Strong)
            .await
            .map(|_| ()),
        refused()
    );
    assert_eq!(service.pin(NS, "al").await.map(|_| ()), refused());
    assert_eq!(
        service
            .get(
                NS,
                "al",
                &[PrimaryKey::U64(1)],
                &Projection::default(),
                ReadConsistency::Strong
            )
            .await
            .map(|_| ()),
        refused()
    );
    // A name that is nothing is still NotFound.
    assert_eq!(
        service.pin(NS, "nothing").await.map(|_| ()),
        Err(not_found("nothing"))
    );
    // With one member left, the alias resolves again.
    service
        .update_alias_targets(
            NS,
            vec![AliasTargetAction::Remove {
                alias: "al".to_string(),
                collection: "a".to_string(),
            }],
        )
        .await
        .expect("shrink");
    assert_eq!(
        service.get_collection(NS, "al").await.map(|info| info.name),
        Ok("b".to_string())
    );
    f.shutdown().await;
}

#[tokio::test]
async fn update_alias_targets_maps_errors() {
    let (f, service) = with_collections(&["a", "b"]).await;
    assert_eq!(
        service
            .update_alias_targets(NS, vec![add("b", "a", None)])
            .await,
        Err(ServiceError::AlreadyExists("b".to_string()))
    );
    assert_eq!(
        service
            .update_alias_targets(NS, vec![add("al", "missing", None)])
            .await,
        Err(not_found("missing"))
    );
    assert_eq!(
        service
            .update_alias_targets(
                NS,
                vec![add("al", "a", Some(true)), add("al", "b", Some(true))]
            )
            .await,
        Err(ServiceError::InvalidArgument(
            "alias [al] has more than one write index [a,b]".to_string()
        ))
    );
    assert!(matches!(
        service.update_alias_targets(NS, Vec::new()).await,
        Err(ServiceError::InvalidArgument(_))
    ));
    // An absent namespace: an Add names a missing collection; removals are
    // no-ops.
    assert_eq!(
        service
            .update_alias_targets("nowhere", vec![add("al", "a", None)])
            .await,
        Err(not_found("a"))
    );
    assert_eq!(
        service
            .update_alias_targets(
                "nowhere",
                vec![AliasTargetAction::RemoveAlias {
                    alias: "al".to_string()
                }]
            )
            .await,
        Ok(())
    );
    f.shutdown().await;
}

#[tokio::test]
async fn collection_info_lists_every_alias_of_the_collection() {
    let (f, service) = with_collections(&["a", "b"]).await;
    service
        .update_aliases(
            NS,
            vec![AliasAction::Create {
                alias: "only-a".to_string(),
                collection: "a".to_string(),
            }],
        )
        .await
        .expect("single-target alias");
    service
        .update_alias_targets(
            NS,
            vec![
                add("both", "a", None),
                add("both", "b", Some(true)),
                add("set-a", "a", Some(false)),
            ],
        )
        .await
        .expect("aliases");
    let a = service.get_collection(NS, "a").await.expect("info a");
    assert_eq!(a.aliases, vec!["both", "only-a", "set-a"]);
    let b = service.get_collection(NS, "b").await.expect("info b");
    assert_eq!(b.aliases, vec!["both"]);
    let listed = service.list_collections(NS).await.expect("list");
    assert_eq!(
        listed
            .iter()
            .map(|info| (info.name.as_str(), info.aliases.clone()))
            .collect::<Vec<_>>(),
        vec![
            (
                "a",
                vec![
                    "both".to_string(),
                    "only-a".to_string(),
                    "set-a".to_string()
                ]
            ),
            ("b", vec!["both".to_string()]),
        ]
    );
    f.shutdown().await;
}

#[tokio::test]
async fn sql_tables_leave_out_multi_target_aliases() {
    let (f, service) = with_collections(&["a", "b"]).await;
    service
        .update_alias_targets(
            NS,
            vec![
                add("one", "a", Some(true)),
                add("two", "a", None),
                add("two", "b", None),
            ],
        )
        .await
        .expect("aliases");
    // SQL addresses one collection per table: `one` is a table, `two` is not.
    let deadline = Instant::now() + WAIT;
    loop {
        service.catalog().refresh(NS).await.expect("refresh");
        let names = service.catalog().names(NS);
        if names == ["a", "b", "one"] {
            break;
        }
        assert!(Instant::now() < deadline, "catalog names: {names:?}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        service.catalog().collection(NS, "one").map(|c| c.name),
        Some("a".to_string())
    );
    assert_eq!(service.catalog().collection(NS, "two"), None);
    f.shutdown().await;
}
