# Data Pipeline Engineer Test

One of the most important parts of the job is to move data from one place to another, and having it run regularly to keep the two data sources in sync.

For this test, you are tasked with extracting a dataset from a corpus of HTML files and storing it for use in a downstream task. You can use
the programming language you feel most comfortable with to complete the task, but we have supplied some utility functions -
written in Python - in `utils.py`. You can use them as a hint or, after installing the relevant dependencies, execute them as is.

There are 4 sub tasks. Try to do as much as you can in around 2 hours.

## 1. Extract and manipulate data

Using the lookup data in `commoncrawl_lookup.csv`, you should extract information about each node's tags in the HTML trees.
In particular, for each node in each HTML page, we need its tag, the tag of its left and right siblings, and the tag of
its parent. The utility function `load_single_warc_record` will allow you to download the HTML and the `get_*` functions 
will should help you extract the relevant columns (but you will have to implement one of those functions yourself).

## 2. Store in a database

Record all this information in an SQLite3 database. As a minimum, you should create and populate these tables:

  1. `webpage` for storing data about the website / HTML. Namely, the URL, but also anything else you find important
  2. `tags` for storing the four extracted tag columns and anything else you find important

As part of your assessment, we ask that you supply the SQLite3 database file containing extracted data in the relevant
tables.

**Note**

The script used to upload the data to the database should be able to deal with new data that has been extracted by the script in part 1. The requirements are

1. It should not upload duplicate data again.
2. If the tags of a URL change it should not overwrite existing data.
3. New URLs and corresponding tags should be inserted if found.

## 3. Dockerize

Please write a Dockerfile that can be used to run your code end-to-end. That is, it must perform steps 1) and 2) above. To test your solution, we will run your Dockerfile with multiple files like `commoncrawl_lookup.csv` to make sure duplicates and new data is being handled correctly.

Write an accompanying script containing the exact `docker build` and `docker run` commands for that Dockerfile.

## 4. CI/CD

### 4a. Docker container

Write a CI workflow to build and deploy the docker container from the Dockerfile in step 3. You can use [Github Actions](https://github.com/features/actions) for this.

### 4b. Orchestration

The docker container should be run daily. We use Kubernetes for orchestration, and if you have experience of Kube please write a manifest that will run this docker container on a daily basis.

## Result

You should make a GitHub repository containing the code you developed for this task, structuring it in a sensible way. If you choose not to commit the file containing your SQLite database, please send it to us as an attachment along with the link to your GitHub repo. 

Good luck!

Supplied to you:
  - `commoncrawl_lookup.csv`
  - `utils.py`
  - `task.md` (this file)

Required by us:
  - Data:
    - SQLite3 database file produced by your code
    
  - Code:
    - Extraction / storage script(s)
    - Dockerfile
    - Script with the `docker build` and `docker run` commands
    - Kubernetes manifest
    - CI workflow

Resources:

  - https://en.wikipedia.org/wiki/Document_Object_Model
  - https://www.w3schools.com/html/html_elements.asp
  - https://www.digitalocean.com/community/tutorials/how-to-install-and-use-sqlite-on-ubuntu-20-04
  - https://docs.docker.com/engine/reference/builder/
  - https://kubernetes.io/docs/home/
  - https://aws.amazon.com/eks/
