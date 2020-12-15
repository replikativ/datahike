# Tech Preview: 



## Clojurescript and IndexedDB support for Datahike

This branch contains initial work on Clojurescript support for Datahike with persistence to IndexedDB in the browser.



The two namespaces of interest are:

- `api_sandbox_indexeddb.cljs`
- `api_sandbox_mem.cljs`



You will need **Clojure** and **shadow-cljs** installed.  

The settings for starting your clojurescript repl is as follows:

- Project type: `shadow-cljs`

- Build selection: `:app`

- Build to connect to: `browser-repl`



## What can I do with this preview?

This tech preview serves as a big milestone for Datahike. As such we wanted to share it with the community early. 

**Included:**

- Create a store
  - In memory
  - IndexedDB
- Transact
- Query
- Entity (working on the API)

**Not yet included:**

- History functionality
- Pull
- Filter



## Where to from here?

We are going to be rebasing against the master branch of Datahike which now has upserts and tuple support. We will then continue to port the remainder of the Datahike API to Clojurescript. At the same time we will be working towards the choice to run Datahike in a web worker. We look forward to feedback from the community and have provided ways to contribute below. We want to work closely with those interested in having Datahike in the browser. Please feel free to reach out to us on Discord 🙂



## Ways to contribute

- Report issues and star us on Github

- [Join us on Discord](https://discord.com/invite/kEBzMvb)

- [Support us on Open Collective](https://opencollective.com/datahike)

