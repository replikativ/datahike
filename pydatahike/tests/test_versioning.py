"""End-to-end versioning tests for the new branching API surface.

Covers:
  - branches / branch! / delete-branch!
  - commit-id / parent-commit-ids
  - merge-db (with parents tracked)
  - input_format='branch:NAME' and 'commit:UUID' for loading the DB at a
    branch head or specific commit

These are deliberately written as a workflow test, not per-op micro-tests:
the goal is to verify that the full git-like cycle (branch → fork-transact
→ diff via Datalog → merge) works end-to-end, since that's the user-facing
contract the rest of the API leans on.
"""
import json
import uuid as _uuid

from datahike import (
    create_database,
    delete_database,
    database_exists,
    transact,
    q,
    branches,
    branch as branch_bang,
    delete_branch,
    commit_id,
    parent_commit_ids,
    merge_db,
)


def _config(branch=None):
    """Build a fresh in-memory config (optionally pinned to a branch)."""
    cfg = '{:store {:backend :memory :id #uuid "' + str(_uuid.uuid4()) + '"}}'
    return cfg


def _unwrap(result):
    """Unwrap !set / !kw / !uuid wrappers from JSON-formatted results."""
    if isinstance(result, list) and len(result) == 2:
        tag = result[0]
        if tag == '!set':
            return [_unwrap(x) for x in result[1]]
        if tag in ('!kw', '!uuid'):
            return result[1]
    if isinstance(result, list):
        return [_unwrap(x) for x in result]
    return result


def test_create_list_delete_branch():
    """branch! / branches / delete-branch! basic lifecycle."""
    cfg = _config()
    create_database(cfg)
    try:
        # The default :db branch is always present.
        names = _unwrap(branches(cfg, output_format='json'))
        assert 'db' in names

        # Branch :feature from :db
        branch_bang(cfg, 'db', 'feature')
        names = _unwrap(branches(cfg, output_format='json'))
        assert 'feature' in names

        delete_branch(cfg, 'feature')
        names = _unwrap(branches(cfg, output_format='json'))
        assert 'feature' not in names
    finally:
        delete_database(cfg)


def test_branch_from_commit_uuid():
    """branch! accepts a commit UUID as the source, not just a branch name."""
    cfg = _config()
    create_database(cfg)
    try:
        transact(cfg, '[{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}]',
                 input_format='edn')

        # Capture the current commit id (returned as ['!uuid', '<uuid>'] in JSON)
        cid_raw = commit_id(cfg, output_format='json')
        cid = _unwrap(cid_raw)
        assert isinstance(cid, str)
        # Sanity: it round-trips as a UUID
        _uuid.UUID(cid)

        # Branch from that commit id (not a branch name)
        branch_bang(cfg, cid, 'snapshot')
        names = _unwrap(branches(cfg, output_format='json'))
        assert 'snapshot' in names
    finally:
        delete_database(cfg)


def test_load_db_at_branch_via_input_format():
    """input_format='branch:NAME' loads the db at a specific branch head."""
    cfg = _config()
    create_database(cfg)
    try:
        transact(cfg, '[{:db/ident :widget/sku :db/valueType :db.type/string '
                      ':db/cardinality :db.cardinality/one :db/unique :db.unique/identity}]',
                 input_format='edn')
        transact(cfg, '[{:widget/sku "A"}]', input_format='edn')

        branch_bang(cfg, 'db', 'feature')

        # Add another widget on :feature
        feature_cfg = cfg.replace('}}', '} :branch :feature}')
        transact(feature_cfg, '[{:widget/sku "B"}]', input_format='edn')

        # Query :feature via input_format='branch:feature'
        result = q('[:find ?sku :where [?e :widget/sku ?sku]]',
                   [('branch:feature', cfg)],
                   output_format='json')
        feature_skus = {row[0] for row in _unwrap(result)}
        assert feature_skus == {'A', 'B'}

        # :db remains untouched
        main_result = q('[:find ?sku :where [?e :widget/sku ?sku]]',
                        [('db', cfg)],
                        output_format='json')
        main_skus = {row[0] for row in _unwrap(main_result)}
        assert main_skus == {'A'}
    finally:
        delete_database(cfg)


def test_load_db_at_commit_via_input_format():
    """input_format='commit:UUID' loads the db at a specific historical commit."""
    cfg = _config()
    create_database(cfg)
    try:
        transact(cfg, '[{:db/ident :widget/sku :db/valueType :db.type/string '
                      ':db/cardinality :db.cardinality/one :db/unique :db.unique/identity}]',
                 input_format='edn')
        transact(cfg, '[{:widget/sku "A"}]', input_format='edn')

        # Pin this commit id
        before_cid = _unwrap(commit_id(cfg, output_format='json'))

        # Mutate
        transact(cfg, '[{:widget/sku "B"}]', input_format='edn')

        # Reading at the pinned commit shows only "A"
        result = q('[:find ?sku :where [?e :widget/sku ?sku]]',
                   [('commit:' + before_cid, cfg)],
                   output_format='json')
        pinned_skus = {row[0] for row in _unwrap(result)}
        assert pinned_skus == {'A'}

        # Reading at HEAD shows both
        head_result = q('[:find ?sku :where [?e :widget/sku ?sku]]',
                        [('db', cfg)],
                        output_format='json')
        head_skus = {row[0] for row in _unwrap(head_result)}
        assert head_skus == {'A', 'B'}
    finally:
        delete_database(cfg)


def test_merge_records_multi_parent_commit():
    """merge-db records a commit whose parent-commit-ids includes both branches."""
    cfg = _config()
    create_database(cfg)
    try:
        transact(cfg, '[{:db/ident :widget/sku :db/valueType :db.type/string '
                      ':db/cardinality :db.cardinality/one :db/unique :db.unique/identity}]',
                 input_format='edn')
        transact(cfg, '[{:widget/sku "A"}]', input_format='edn')

        # Branch and add data on :feature
        branch_bang(cfg, 'db', 'feature')
        feature_cfg = cfg.replace('}}', '} :branch :feature}')
        transact(feature_cfg, '[{:widget/sku "B"}]', input_format='edn')

        # Merge :feature's new datom into :db
        merge_db(cfg, ['feature'], '[{:widget/sku "B"}]', input_format='edn')

        # :db now sees both rows
        result = q('[:find ?sku :where [?e :widget/sku ?sku]]',
                   [('db', cfg)],
                   output_format='json')
        skus = {row[0] for row in _unwrap(result)}
        assert skus == {'A', 'B'}

        # The merge commit has multiple parents.
        parents_raw = parent_commit_ids(cfg, output_format='json')
        parents = _unwrap(parents_raw)
        assert isinstance(parents, list)
        assert len(parents) >= 2, f"Expected ≥ 2 parents on merge commit, got {parents}"
    finally:
        delete_database(cfg)
