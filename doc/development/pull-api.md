# Pull-API: Terminology


## Selector

Quoted vector holding one or several of the following expressions:
1) wildcard
   - pulls all datoms for entity id
   - e.g. * -> {:db/id 1 :name "Petr" :child [{:db/id 2}]}
2) keyword for
   - attributes to get
     - e.g. :child (= has child) -> {:child [#:db{:id 10}]}
   - reverse attributes to get (datom where requested entity is referenced)
     - signaled by underscore
     - e.g. :_child (= child of) -> {:_child [#:db{:id 1}]}
3) map of keywords for (reverse) attributes to get and
   - vectors of attributes
     - attribute should be reference
     - e.g. {:child [:name :age]} -> {:child {:name "Thomas" :age 10}}
     - e.g. {:_child [:name :age]} -> {:_child {:name "Charles" :age 10}}
   - three dots
     - for infinite recursion on attribute
   - number
     - for finite recursion on attribute
4) function call form:
   - limit:
     - restricts amount of pulled datoms to given number
     - e.g. (limit :child 2)
   - defaults:
     - sets a default value if search returns nil
     - e.g. (default :name "unknown")
5) vectors of 3
   - functions of 4) in vector form
     - attribute, function name as keyword, parameter
     - e.g. [:child :limit 2]
            [:name :default "unknown"]
   - renaming of attributes with :as
     - attribute, :as, new name
     - e.g. [:aka :as :alias]
            [:name :as "Name"]

## Pattern = (Pull-)Spec

Record with keys
- :wildcard? = boolean value indicating if wildcard expansion has to be done
- :attrs = map of attribute names as in selector and map with keys
           - :attr = attribute name as in database; only different from key when key describes reverse attribute
           - :subpattern (opt) = spec of subpattern applied to attribute
           - :recursion = always nil, only checks if key exists, if it does: recursion is applied on attribute
           - :limit = amount of maximum number of results returned
           - :default = default value if nothing found


## Frame

Map with keys
  - :multi? = true if multiple entities requested
  - :eids = vector of eids to pull pattern for
  - :state = pattern/expand/expand-rev/recursion/done
  - :pattern = (Pull-)Spec
  - :recursion = map with keys :depth (depth per attribute) and :seen (eids)
  - :specs = attrs from spec
  - :wildcard? = wildcard from spec
  - :kvps = final result map; in pull-pattern transfered to next frame when current frame is done
  - :results = result map for current frame, i.e. for current attribute
  - :attr = optional; current attribute (pull-attr-datoms) or enum, e.g. recursion
  - :datoms = optional; datoms pulled from database; e.g. added by wildcard-expand
  - :expand-kvps = optional; added by expand-frame, used by expand-rev-frame

### Frame Creation and State Changes

- only 1 frame created for basic input pattern
- additional frames added on processing of
  - recursion: 2 frames per depth
  - wildcard: 1 frame
  
![Frame State Diagram](https://raw.githubusercontent.com/replikativ/datahike/documentation/doc/development/pull-frame-state-diagram.jpg)


