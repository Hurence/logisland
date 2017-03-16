Welcome to the LogIsland documentation!
=======================================

This readme will walk you through navigating and building the LogIsland documentation, which is included
here with the  source code. 

Read on to learn more about viewing documentation in plain text (i.e., markdown) or building the
documentation yourself. Why build it yourself? So that you have the docs that corresponds to
whichever version of LogIsland you currently have checked out of revision control.

Prerequisites
-------------
The LogIsland documentation build uses `Sphinx <ttp://www.sphinx-doc.org/en/1.5.1/>`_
To get started you can run the following commands

    $ sudo pip install Sphinx


## Generating the Documentation HTML

We include the LogIsland documentation as part of the source (as opposed to using a hosted wiki, such as
the github wiki, as the definitive documentation) to enable the documentation to evolve along with
the source code and be captured by revision control (currently git). This way the code automatically
includes the version of the documentation that is relevant regardless of which version or release
you have checked out or downloaded.

In this directory you will find textfiles formatted using ReSTructured, with an ".rst" suffix. You can
read those text files directly if you want.

Execute `jekyll build` from the `docs/` directory to compile the site. Compiling the site with
Jekyll will create a directory called `_site` containing index.html as well as the rest of the
compiled files.

    $ cd logisland-docs
    $ make html

