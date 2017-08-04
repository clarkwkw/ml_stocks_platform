defaultpyint=$(which python)
read -p "Python interpreter [default=$defaultpyint]: " pyint
pyint=${pyint:=$defaultpyint}
zip -rq mlsp.zip test_scripts DataPreparation MachineLearningModelUtilities PortfolioConstructionUtilities __main__.py config.py debug.py test.py
echo "#!$pyint" | cat - mlsp.zip > mlsp
rm mlsp.zip
chmod +x mlsp