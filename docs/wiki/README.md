# Wiki source

These files are the source of the [Loams wiki](https://github.com/ostrium-labs/loams/wiki). GitHub only creates the wiki repository after its first page is saved in the UI. Once that is done, publish these pages:

```sh
git clone git@github.com:ostrium-labs/loams.wiki.git ~/Documents/loams-wiki
cp docs/wiki/*.md ~/Documents/loams-wiki/ && rm ~/Documents/loams-wiki/README.md
cd ~/Documents/loams-wiki && git add -A && git commit -s -m "wiki: initial pages" && git push
```

The pages are short and link to the repository docs, which stay the source of truth.
