# **Schneewittchens Stiefmutter** - Automatic Question Generation out of Texts
"Schneewittchen's Stiefmutter" (English: "Snow White's Stepmother") was developed as part of my master thesis. It is a tool that automatically generates never-before seen questions out of texts and is built with data from [Wikidata](https://www.wikidata.org/wiki/Wikidata:Main_Page).
The tool's name is a hommage to the well-known German fairytale "Schneewittchen und die sieben Zwerge" (Snow White and the Seven Dwarfs) by the brother's Grimm. In the fairytale, Snow White's stepmother -- the evil queen -- is questioning her magical mirror to receive answers. The tool is not an oracle like the mirror, but rather generates questions which the evil queen could ask, thus, it is called "Schneewittchen's Stiefmutter".

# System requirements for data extraction
* Python 3.7
* Python packages (use pip to install them):
	* bs4
    * locale and the language-package 'de_DE.utf8' (run in a bash: $ sudo locale-gen de_DE.utf8 and $ update-locale LANG=de_DE.UTF-8)
	* spacy
    * psycopg2
	* unidecode (In order to parse German texts, it is necessary to adapt two files (to keep German Umlaute). On your computer: navigate to the Python-bin-folder, there to > site-packages and to > unidecode and replace 'x000.py' and 'x020.py' with the two files attached. )
* PostgreSQL installed on your computer
	
# System requirements for question generation
* a webbrowser (preferably, Google Chrome)

# Preparation & Execution for data extraction
* Set up a new PostgreSQL-database on your computer by following the steps as described [here](https://www.microfocus.com/documentation/idol/IDOL_12_0/MediaServer/Guides/html/English/Content/Getting_Started/Configure/_TRN_Set_up_PostgreSQL_Linux.htm) or alike.
* Edit the file [Code/src/database.ini](https://github.com/sflin/SchneewittchensStiefmutter/blob/master/Code/src/database.ini) and adapt it with your credentials and database-name.
* Go to [Wikidata](https://www.wikidata.org/wiki/Wikidata:Main_Page) and select a set of Q-Objects as initial seeds (between 1 and 10 IDs).
* Open the file [Code/src/extraction_pipeline.py](https://github.com/sflin/SchneewittchensStiefmutter/blob/master/Code/src/extraction_pipeline.py) and replace line 138 with your seeds:
```
seeds = [[42],[34660],[5879],[1339],[1299],[47875],[584],[513],[207773],[1374]]
```
* Execute the file [Code/src/database.py](https://github.com/sflin/SchneewittchensStiefmutter/blob/master/Code/src/database.py) and then the file [Code/src/extraction_pipeline.py](https://github.com/sflin/SchneewittchensStiefmutter/blob/master/Code/src/extraction_pipeline.py). While the extraction-pipeline is running, you can check its progress in the generated log file and examine your extracted data in the database.
* If you wish to train a model with your new data: execute the file [Code/src/db-select.py](https://github.com/sflin/SchneewittchensStiefmutter/blob/master/Code/src/db-select.py) and the two file df-types.parquet.gzip and df-triplets.parquet.gzip will be generated. For instance, you could then continue with executing the notebook [Ktrain-TripletExtractor.ipynb](https://github.com/sflin/SchneewittchensStiefmutter/blob/master/Ktrain-TripletExtractor.ipynb).

# Preparation & Execution for question generation
* Please open the [SchneewittchensStiefmutter.ipynb](https://github.com/sflin/SchneewittchensStiefmutter/blob/master/SchneewittchensStiefmutter.ipynb) in google colab and follow the instructions in the notebook. 

# Important notes
* Please note: this tool was implemented using Ubuntu Linux LTS 18.04 thus the installation steps and system requirements only hold for this operation system. I presume the instructions apply for MacOS as well but I give no guarantee for Windows-user.
<img src="https://upload.wikimedia.org/wikipedia/commons/3/36/Wikidata_stamp_rec_light.png" alt="" data-canonical-src="https://upload.wikimedia.org/wikipedia/commons/3/36/Wikidata_stamp_rec_light.png" width="210" height="57" />

# License and Author
Selin Fabel 
selin.fabel@uzh.ch
