read -p "Python interpreter: " pyint
zip -rq mlsp.zip DataPreparation MachineLearningModelUtilities PortfolioConstructionUtilities __main__.py config.py debug.py test.py
echo "#!$pyint" | cat - mlsp.zip > mlsp
rm mlsp.zip
chmod +x mlsp