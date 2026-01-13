# Why Datalog?

Datalog is a declarative query language that excels at expressing complex relationships in graph-structured data. While SQL handles tabular data well, modern applications increasingly need to model interconnected entities—social networks, organizational hierarchies, supply chains, knowledge graphs.

## When SQL Falls Short

**The problem**: As applications evolve, relational schemas accumulate join complexity. What starts as simple tables becomes a maze of foreign keys, junction tables, and nested subqueries. Developers spend more time managing joins than expressing business logic.

**Common pattern**: Start with SQL for simplicity → encounter complex relationships → add more joins → build ad-hoc graph features → end up with fragile, hard-to-maintain queries.

**Datalog's approach**: Pattern matching over relationships. Instead of explicitly specifying how to join tables, you describe what patterns you're looking for. The query engine handles traversal.

## Query Comparison

Let's find all projects where someone manages a team member who contributed to that project.

### SQL Approach

```sql
SELECT DISTINCT
    m.name AS manager_name,
    e.name AS employee_name,
    p.name AS project_name
FROM employees m
JOIN manager_relationships mr ON m.id = mr.manager_id
JOIN employees e ON mr.employee_id = e.id
JOIN project_contributors pc ON e.id = pc.employee_id
JOIN projects p ON pc.project_id = p.id
WHERE m.id != e.id;
```

This requires:
- Understanding the table structure (4 tables, 3 join conditions)
- Explicit join order management
- Mental model of how data flows through joins

### Datalog Approach

```clojure
(d/q '[:find ?manager-name ?employee-name ?project-name
       :where
       [?manager :employee/manages ?employee]
       [?employee :project/contributed-to ?project]
       [?manager :employee/name ?manager-name]
       [?employee :employee/name ?employee-name]
       [?project :project/name ?project-name]]
     db)
```

Pattern matching expresses intent:
- "Find managers who manage employees"
- "Find employees who contributed to projects"
- "Get their names"

No explicit joins. No table knowledge required. The relationships are first-class.

## More Complex: Transitive Relationships

Find all skills accessible through management chain (managers inherit team skills).

### SQL

```sql
WITH RECURSIVE management_chain AS (
    -- Base case: direct reports
    SELECT manager_id, employee_id, 1 as depth
    FROM manager_relationships

    UNION ALL

    -- Recursive case: indirect reports
    SELECT mc.manager_id, mr.employee_id, mc.depth + 1
    FROM management_chain mc
    JOIN manager_relationships mr ON mc.employee_id = mr.manager_id
    WHERE mc.depth < 10  -- Prevent infinite recursion
)
SELECT DISTINCT
    m.name AS manager_name,
    s.name AS accessible_skill
FROM employees m
JOIN management_chain mc ON m.id = mc.manager_id
JOIN employee_skills es ON mc.employee_id = es.employee_id
JOIN skills s ON es.skill_id = s.id;
```

Requires:
- Understanding recursive CTEs
- Managing recursion depth manually
- Multiple joins to assemble result

### Datalog with Rules

```clojure
;; Define a rule for transitive management
(d/q '[:find ?manager-name ?skill-name
       :in $ %
       :where
       (manages-recursively ?manager ?employee)
       [?employee :employee/has-skill ?skill]
       [?manager :employee/name ?manager-name]
       [?skill :skill/name ?skill-name]]
     db
     '[;; Rule definition
       [(manages-recursively ?m ?e)
        [?m :employee/manages ?e]]
       [(manages-recursively ?m ?e)
        [?m :employee/manages ?x]
        (manages-recursively ?x ?e)]])
```

**Rules encapsulate logic**: `manages-recursively` handles the transitive relationship. Reusable across queries.

## Graph Traversal: Friend Recommendations

Find friends-of-friends who share interests, excluding existing friends.

### SQL

```sql
SELECT DISTINCT
    p1.name AS person_name,
    p3.name AS recommendation_name,
    i.name AS shared_interest
FROM people p1
JOIN friendships f1 ON p1.id = f1.person_id
JOIN people p2 ON f1.friend_id = p2.id
JOIN friendships f2 ON p2.id = f2.person_id
JOIN people p3 ON f2.friend_id = p3.id
JOIN person_interests pi1 ON p1.id = pi1.person_id
JOIN person_interests pi3 ON p3.id = pi3.person_id
JOIN interests i ON pi1.interest_id = i.id AND pi3.interest_id = i.id
LEFT JOIN friendships f_check ON (p1.id = f_check.person_id AND p3.id = f_check.friend_id)
WHERE p1.id != p3.id
  AND f_check.person_id IS NULL;  -- Not already friends
```

8 joins, complex exclusion logic, hard to maintain.

### Datalog

```clojure
(d/q '[:find ?person-name ?recommendation-name ?interest-name
       :where
       [?person :person/friend ?friend]
       [?friend :person/friend ?recommendation]
       [?person :person/interest ?interest]
       [?recommendation :person/interest ?interest]
       [(not= ?person ?recommendation)]
       (not [?person :person/friend ?recommendation])
       [?person :person/name ?person-name]
       [?recommendation :person/name ?recommendation-name]
       [?interest :interest/name ?interest-name]]
     db)
```

**Natural expression**: Each line describes one relationship pattern. Negation uses `not` clause rather than LEFT JOIN tricks.

## Multi-Database Queries

Datalog can query across multiple databases in a single query—useful for data federation or comparing environments.

```clojure
;; Compare production and staging: Find entities in staging not in production
(d/q '[:find ?name ?value
       :in $staging $prod
       :where
       [$staging ?e :entity/name ?name]
       [$staging ?e :entity/value ?value]
       (not [$prod ?e2 :entity/name ?name])]
     staging-db prod-db)
```

SQL would require UNION queries, temporary tables, or separate queries with application-level comparison.

## When to Use Datalog

**Choose Datalog when:**
- Modeling interconnected entities (social graphs, org charts, supply chains)
- Need recursive or transitive queries
- Schema evolves frequently (schema-less or flexible schema)
- Want to express logic through rules
- Query across multiple databases or time points
- Building knowledge graphs or semantic systems

**SQL still wins for:**
- Pure tabular data with minimal relationships
- Standard BI tool integration
- Window functions and complex aggregations over large sorted sets
- Existing SQL infrastructure and expertise

## Performance Note

Datalog query engines (including Datahike) use sophisticated join optimization. Performance is comparable to well-tuned SQL for most queries. For specific workloads, see our [benchmarks](https://gitlab.com/arbetsformedlingen/taxonomy-dev/backend/experimental/datahike-benchmark/).

## Learning Datalog

Datalog syntax is minimal:
- Pattern matching: `[?entity :attribute ?value]`
- Rules: Define reusable logic patterns
- Negation: `(not ...)`
- Functions: `[(function arg) result]`

Most developers become productive within hours. The conceptual shift from imperative joins to declarative patterns is the main learning curve, not syntax complexity.

## Further Reading

- [Datalog Wikipedia](https://en.wikipedia.org/wiki/Datalog) - Academic background
- [Learn Datalog Today](http://www.learndatalogtoday.org/) - Interactive tutorial
- [Datomic Query Documentation](https://docs.datomic.com/query/query.html) - Datahike uses compatible query syntax
- [Datahike Query Guide](https://cljdoc.org/d/io.replikativ/datahike/CURRENT/doc/query) - Full query capabilities
