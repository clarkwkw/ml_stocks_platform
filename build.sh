defaultpyint=$(which python)
read -p "Python interpreter [default=$defaultpyint]: " pyint
pyint=${pyint:=$defaultpyint}
zip -rq mlsp.zip ConfigFilesUtilities DataPreparation MachineLearningModelUtilities PortfolioConstructionUtilities __main__.py config.py debug.py json_utils.py
echo "#!$pyint" | cat - mlsp.zip > mlsp
rm mlsp.zip
chmod +x mlsp