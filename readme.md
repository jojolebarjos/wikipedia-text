
# Wikipedia raw text

This script extract plain text sentences from [Wikipedia ZIM dumps](https://dumps.wikimedia.org/other/kiwix/zim/wikipedia/).

```
pip install lxml tqdm
wget https://dumps.wikimedia.org/other/kiwix/zim/wikipedia/wikipedia_en_all_nopic_2017-08.zim
python extract.py wikipedia_en_all_nopic_2017-08.zim en.xml.gz en
```
