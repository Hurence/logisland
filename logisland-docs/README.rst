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

    pip install -r requirements.txt
    sudo pip install Sphinx


Generating the Documentation HTML
---------------------------------

We include the LogIsland documentation as part of the source (as opposed to using a hosted wiki, such as
the github wiki, as the definitive documentation) to enable the documentation to evolve along with
the source code and be captured by revision control (currently git). This way the code automatically
includes the version of the documentation that is relevant regardless of which version or release
you have checked out or downloaded.


This documentation is built using [Sphinx](http://sphinx-doc.org). It also uses some extensions for theming and REST API
documentation support.

Start by installing the requirements:

    pip install -r requirements.txt

Then you can generate the HTML version of the docs:

    make html

The root of the documentation will be at `_build/html/index.html`

While editing the documentation, you can get a live preview using python-livepreview. Install the Python library:

    pip install livereload

Then run the monitoring script in the background:

    python autoreload.py &

If you install the [browser extensions](http://livereload.com/) then everything should update every time any files are
saved without any manual steps on your part.