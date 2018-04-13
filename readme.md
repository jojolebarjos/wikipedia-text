
# Wikipedia raw text

This script extracts plain text sentences from [Wikipedia ZIM dumps](https://dumps.wikimedia.org/other/kiwix/zim/wikipedia/). Complex or irrelevant structures are ignored (e.g. images and tables) and text formatting is simplified (e.g. emphasis are removed).

```
pip install lxml tqdm
wget https://dumps.wikimedia.org/other/kiwix/zim/wikipedia/wikipedia_en_all_nopic_2017-08.zim
python extract.py wikipedia_en_all_nopic_2017-08.zim en.xml.gz en
```

Additionally, plain text can be extracted using the following command:

```
python convert.py en.xml.gz en.txt
```
