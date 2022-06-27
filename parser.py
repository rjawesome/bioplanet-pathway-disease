import csv
import os

#convert csv into a list of dicts
def read_csv(file: str, delim: str):
  info = []
  with open(file) as csv_file:
    reader = csv.reader(csv_file, delimiter=delim)
    categories: list[str]
    i = 0
    for row in reader:
      if len(row) == 0:
        continue
      if i == 0:
        categories = row
      else:
        info.append({})
        for j in range(len(row)):
          info[i-1][categories[j]] = row[j]
      i += 1
  
  return info

def load_data(data_folder: str):
  #read files
  pathway_disease_info = read_csv(os.path.join(data_folder, "pathway-disease-mapping.tsv"), "\t")
  pathway_category_info = read_csv(os.path.join(data_folder, "pathway-category.csv"), ",")

  #get pathway categories
  pathway_categories = {}

  for row in pathway_category_info:
    row_id = row['PATHWAY_ID']
    if not row_id in pathway_categories:
      pathway_categories[row_id] = []
    pathway_categories[row_id].append(row['CATEGORY_NAME'])

  docs: dict[str, any] = {}

  #create json for pathway/disease
  for row in pathway_disease_info:
    doc_id = f"{row['PATHWAY_ID']}-{row['DISEASE_NAME']}"

    #just append gene if we have already processed pathway-disease pair
    if doc_id in docs:
      docs[doc_id]['relation'].append({
        'GENE_ID': int(row['GENE_ID']),
        'GENE_SYMBOL': row['GENE_SYMBOL'],
        'MIM_ID': int(row['MIM_ID'])
      })
    else:
      docs[doc_id] = {
        '_id': doc_id,
        'subject': {
          'DISEASE_NAME': row['DISEASE_NAME'],
          'PHENO_TYPE': -1 if row['PHENO_TYPE'] == '' else int(row['PHENO_TYPE'])
        },
        'object': {
          'PATHWAY_ID': row['PATHWAY_ID'],
          'PATHWAY_NAME': row['PATHWAY_NAME'],
          'PATHWAY_CATEGORIES': pathway_categories[row['PATHWAY_ID']]
        },
        'relation': [{
          'GENE_ID': int(row['GENE_ID']),
          'GENE_SYMBOL': row['GENE_SYMBOL'],
          'MIM_ID': int(row['MIM_ID'])
        }],
        'predicate': 'related_to'
      }
  
  for doc_id in docs:
    yield docs[doc_id]
    

with open("./output.json", "w") as f:
  for i in load_data("./data"):
    f.write(str(i) + "\n")