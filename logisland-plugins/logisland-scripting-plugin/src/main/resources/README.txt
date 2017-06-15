This jar file holds dependencies needed to allow running scripting processors.
Currently, only python is supported thus this jar file only includes python dependencies
and some libraries delivered with Logisland that can be used from python processors.
As of today, only the nltk library is delivered. The modifications done to it are
explained here after:

- AbstractProcessor.py:

This is the mother class a python processor must inherit from (like in java, a processor must inherit from AbstractProcessor).
The name of the python processor script (file mode) must be the name of the class it contains and this class must inherit
from AbstractProcessor.

- nltk:

This is the copy of the nltk v3.2.1 library with the following modidifications:

Workaround as we cannot use sqlite dependency from Jython env
(see http://stackoverflow.com/questions/3875212/sqlite3-module-for-jython for further explainations):

diff for nltk/corpus/reader/__init__.py
---------------------------------------

105c105
< from nltk.corpus.reader.panlex_lite import *
---
> #from nltk.corpus.reader.panlex_lite import *
143,144c143,144
<     'ProsConsCorpusReader', 'CategorizedSentencesCorpusReader',
<     'ComparativeSentencesCorpusReader', 'PanLexLiteCorpusReader'
---
>     'ProsConsCorpusReader', 'CategorizedSentencesCorpusReader'
> #    'ComparativeSentencesCorpusReader', 'PanLexLiteCorpusReader'

diff for nltk/corpus/__init__.py
--------------------------------

87,89c87,89
< comparative_sentences = LazyCorpusLoader(
<     'comparative_sentences', ComparativeSentencesCorpusReader, r'labeledSentences\.txt',
<     encoding='latin-1')
---
> #comparative_sentences = LazyCorpusLoader(
> #    'comparative_sentences', ComparativeSentencesCorpusReader, r'labeledSentences\.txt',
> #    encoding='latin-1')
164,165c164,165
< panlex_lite = LazyCorpusLoader(
<     'panlex_lite', PanLexLiteCorpusReader)
---
> #panlex_lite = LazyCorpusLoader(
> #    'panlex_lite', PanLexLiteCorpusReader)
