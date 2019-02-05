# b-records

Creates b-records for content-first

## Flow

The provided scripts should be called in the following order

* abstracts
  Harvest abstracts and writes them to file
* build_doc2vec_model
  Builds doc2vec model from harvested abstracts,and wirtes model to file
* generate_subjects
  Generates subjects based om the doc2vec model and writes them to file
* report
  Creates human readable report based on subject-file
