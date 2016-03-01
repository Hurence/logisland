Welcome to the LogIsland documentation!

This readme will walk you through navigating and building the LogIsland documentation, which is included
here with the  source code. 

Read on to learn more about viewing documentation in plain text (i.e., markdown) or building the
documentation yourself. Why build it yourself? So that you have the docs that corresponds to
whichever version of LogIsland you currently have checked out of revision control.

## Prerequisites
The LogIsland documentation build uses a number of tools to build HTML docs and API docs in Scala and Java. To get started you can run the following commands

    $ sudo gem install jekyll
    $ sudo gem install jekyll-redirect-from
    $ sudo pip install Pygments
    $ sudo pip install sphinx


## Generating the Documentation HTML

We include the LogIsland documentation as part of the source (as opposed to using a hosted wiki, such as
the github wiki, as the definitive documentation) to enable the documentation to evolve along with
the source code and be captured by revision control (currently git). This way the code automatically
includes the version of the documentation that is relevant regardless of which version or release
you have checked out or downloaded.

In this directory you will find textfiles formatted using Markdown, with an ".md" suffix. You can
read those text files directly if you want. Start with index.md.

Execute `jekyll build` from the `docs/` directory to compile the site. Compiling the site with
Jekyll will create a directory called `_site` containing index.html as well as the rest of the
compiled files.

    $ cd docs
    $ jekyll build

You can modify the default Jekyll build as follows:

    # Skip generating API docs (which takes a while)
    $ SKIP_API=1 jekyll build
    # Serve content locally on port 4000
    $ jekyll serve --watch
    # Build the site with extra features used on the live page
    $ PRODUCTION=1 jekyll build


## API Docs (Scaladoc, Sphinx, roxygen2)

You can build just the LogIsland scaladoc by running 

```
jekyll build; sbt doc;  mv target/scala-2.10/api/ docs/_site/
```
