##Related tags interview question
================================================

A tag is a label provided by the reader of the given document. A document can have one or more tags. 
We would like to know the tags which are related to eachother.  Tags are related to eachother if they occur multiple times together.
  
-------------------------------------------------------

 Please write your scode in Scala and provide a readme file.  Make sure your code is fully tested. We will be assessing both you algorithmic and design skills.
 
 - You are given src/main/resources/tags.csv file as input.
 - The file contains on each line tags as strings separated by comma; each line corresponds to a document. The line can be empty or can contain empty strings.
 - You are also given a config parameter: maximumTagsPerTag (positive integer) which tells you how many related tags should be generated for each tag; you can store this as variable or provide it from the input.
 - Please write the output to the filesystem in CSV format.
  
-------------------------------------------------------


If a document has tags "fruit" and "apple", and there are more other documents which also have "fruit" and "apple" as 
tags, we can say "fruit" and "apple" are related tags, because they occurred multiple times together. To avoid long running computation, 
we provide a limit which tells you how many related tags should be generated for each tag. The output should be tag pairs. 


**Example 1**

 - "doc1" tags => "apple", "fruit", "food"
 - "doc2" tags => "house", "building"
  maximumTagsPerTag: 1
  
 The output for this input if maximumTagsPerTag is "1":
 [("apple", "fruit"), ("fruit", "food"), ("food", "apple"), ("house", "building"), ("building", "house")]

**Example 2**
 
 Input:    [["apple", "fruit", "food"],
           ["house", "building"],
           ["apple", "fruit"],
           ["peach", "fruit", "sweet"],
           ["tower", " "],
           ["sweet", "fruit", "apple"],
           ["house", "home", "apple"],
           ["home", "building"]]
           
maximumTagsPerTag: 2

In this case "apple" is tagged together with "fruit" 3 times, with "food" once, "house" once etc.
You should choose maximum 2 tags per tag, so you should choose "fruit", since it has the highest occurrence number, and than "food", OR "house" etc., because they
have the second highest occurence number. If the tags have the same occurence number, you can pick randomly. 
So the output should contain ("apple", "fruit"), and ("apple", "food").
Let's check the tag "fruit". We will get "apple", and "sweet" as the most common tags. 
So the output should contain ("fruit", "apple"), and ("fruit", "sweet").
We should do the same thing for every other tags.

The output for the given input should be 
[ ("apple", "fruit"), ("apple", "food"), ("fruit", "apple"), ("fruit", "sweet"), ("food", "apple"),
  ("food", "fruit"), ("house", "building"), ("house", "home"), ("building", "house"), ("building", "home"),
  ("peach", "fruit"), ("peach", "sweet"), ("home", "house"), ("home", "apple")
]