from pyspark.sql import SparkSession 

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("GroceryDataAnalysis")\
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    print("\n############### START: OUTPUT ################\n")

    """
    (a) Data Processing
     - Read grocery_data.csv and store the content using Spark RDDs.
     - Strip off trailing spaces and convert item names to lowercase.
     - Find the total number of transactions.
     - Identify the basket with the most items and display the content.
    """

    #Read grocery_data.csv and store using Spark RDDs
    file_path = "../data/grocery_data.csv"
    grocery_rdd = sc.textFile(file_path)
    #Show content and find the total number of transactions
    print("Displaying grocery_data.csv as RDD (Displaying Up To 20 Records):\n")
    print(grocery_rdd.take(20), "\n\n\n")

    #Strip off trailing spaces, change item names to lowercase and map them into respective baskets
    cleaned_rdd = grocery_rdd.map(lambda line: [item.strip().lower() for item in line.split(',')])
    #Show content and find the total number of transactions
    print("Displaying Cleaned Data (Displaying Up To 20 Records):\n")
    print(cleaned_rdd.take(20), "\n")
    print(f"Total Number of Transactions: {cleaned_rdd.count()} in Cleaned Data\n\n\n")

    #Count the number of items in each basket and find the basket with most items
    max_items_basket = cleaned_rdd.map(lambda items: (len(items), items))
    max_items_basket = max_items_basket.max()

    #Show most items basket content and number of items
    print(f"Basket with the Most Items has {max_items_basket[0]} items in it.")
    print("Basket Content:")
    print(max_items_basket[1])

    #Find unique number of items
    unique_items = cleaned_rdd.flatMap(lambda items: items).distinct()
    print("\n\n\nThe Unique Number of Items:", unique_items.count(), "\n\n\n")

    """
    (b) Frequency Analysis
     - Find the top 20 most frequently purchased items, including their occurrences and corresponding percentages.
     - Find the bottom 20 least frequently purchased items, including their occurrences and corresponding percentages.
    """

    #Find top 20 items based on occurence count (Most purchased)
    top_twenty_items = cleaned_rdd.flatMap(lambda items: items) \
          .map(lambda item: (item, 1)) \
          .reduceByKey(lambda a, b: a + b) \
          .takeOrdered(20, key=lambda x: -x[1])
    
    #Calculate 100% value
    total_transactions = cleaned_rdd.count()

    #Show the Top TWENTY (20) most frequently purchased items
    print("Top TWENTY (20) Most Frequently Purchased Items:")
    for item, count in top_twenty_items:
        percentage = (count / total_transactions) * 100
        print(f"Item: {item:<30}, Occurrences: {count:<6}, Percentage: {percentage:<5.2f}%")
    
    #Find bottom 20 items based on occurence count (Least purchased)
    bottom_twenty_items = cleaned_rdd.flatMap(lambda items: items) \
          .map(lambda item: (item, 1)) \
          .reduceByKey(lambda a, b: a + b) \
          .takeOrdered(20, key=lambda x: x[1])

    #Show the Bottom TWENTY (20) most frequently purchased items
    print("\n\n\nBottom TWENTY (20) Least Frequently Purchased Items:")
    for item, count in bottom_twenty_items:
        percentage = (count / total_transactions) * 100
        print(f"Item: {item:<30}, Occurrences: {count:<6}, Percentage: {percentage:<5.2f}%")
    
    """
    (c) Market Basket Analysis (Item Pairing)
     - Assign an index to each transaction (0, 1, …).
     - Find all possible 2-item combinations in the transactions and display them.
     - Identify transaction indices where each item pair appears and compute the frequency of each item pair.
    """
    
    #Assign index for each transaction
    indexed_transactions = cleaned_rdd.zipWithIndex()
    print("\n\n\nAssigned Index For Each Transaction (Displaying Up To 20 Records):\n")
    print(indexed_transactions.take(20), "\n\n\n")

    #Find all combinations of groups of 2 grocery items (item pair X & Y, transaction index)
    item_combinations = indexed_transactions.flatMap(lambda x: [(x[0][i], x[0][j], x[1]) for i in range(len(x[0])) for j in range(i + 1, len(x[0]))])
    #Sort pairs by name
    sorted_item_combinations = item_combinations.map(lambda x: (tuple(sorted([x[0], x[1]])), x[2]))

    #Show content and number of records
    print("All Possible Combinations of 2 Grocery Items (Sorted by Name, Displaying Up To 20 Records):\n")
    #print(sorted_item_combinations.collect())
    print(sorted_item_combinations.take(20))
    print(f"\nNumber of Records: {sorted_item_combinations.count()}\n\n\n")

    #Find the list of transaction indices associated with a given item pair
    print("List of Transaction Indices Associated with a Given Combination:")
    combinations_indices = sorted_item_combinations.groupByKey()

    print("\nShow Content Up To 20 Records Using .collect():")
    #print(combinations_indices.collect())
    print(combinations_indices.take(20))

    #Formatted Display
    selected_combinations_indices = combinations_indices.take(5)
    print("\nFormatted Display Up To 5 Records:")
    for pair, indices in selected_combinations_indices:
        indices_list = list(indices)
        print(f"Item Pair: {pair},\nAssociated Transaction Indices:\n{indices_list}\n")

    print(f"Total Number of Records: {combinations_indices.count()}\n\n\n")

    #Find number of times a given item pair occurs
    print("List of Number of Occurrences Associated with a Given Item Pair (Sorted Highest to Lowest):")
    combinations_counts = combinations_indices.map(lambda x: (x[0], len(x[1]))).sortBy(lambda x: -x[1])
    
    print("\nShow Content Up To 20 Records Using .collect():")
    #print(combinations_counts.collect())
    print(combinations_counts.take(20))

    #Formatted Display
    selected_combinations_counts = combinations_counts.take(5)
    print("\nFormatted Display Up To 5 Records:")
    for pair, total in selected_combinations_counts:
        print(f"Item Pair: {pair}, \nTotal Number of Occurrences: {total} \n")

    print(f"Number of Records: {combinations_counts.count()}\n\n\n")
   
    """
    (d) Support Metric Calculation
     - Compute the support for each item pair based on their frequency and total number of transactions.
     - Sort the item pairs by their occurrence count from highest to lowest.
     - Display the top and bottom 20 item pairs sorted by their occurrence counts.
    """
    
    #Calculate Support Metrics and store it in ((item X, item Y), (occurrence count, Support percentage))
    print("Support Metric For Every Item Pair (Displaying Up To 20 Records):\n")
    support_metrics = combinations_counts.map(lambda x: ((x[0][0], x[0][1]), (x[1], (x[1] / total_transactions) * 100)))
    print(support_metrics.take(20))
    print(f"\nNumber of Records: {support_metrics.count()}\n\n\n")

    #Find the Top TWENTY (20) item pair sorted by occurrence count of item pair X and Y, from highest to lowest.
    print("Top 20 Popular Item Pair (Support Metrics, Sorted By Occurrence Count):\n")
    sorted_top_support_metrics = support_metrics.sortBy(lambda x: -x[1][0])
    top_20_support_metrics = sorted_top_support_metrics.take(20)
    print(top_20_support_metrics)

    #Find the Bottom TWENTY (20) item pair sorted by occurrence count of item pair X and Y, from highest to lowest.
    print("\n\n\nBottom 20 Popular Item Pair (Support Metrics, Sorted By Occurrence Count):\n")
    sorted_bottom_support_metrics = support_metrics.sortBy(lambda x: x[1][0])
    bottom_20_support_metrics = sorted_bottom_support_metrics.take(20)
    print(bottom_20_support_metrics)

    """
    (e) Confidence Metric Calculation
     - Calculate confidence for item pairs based on their frequency in transactions.
     - Find the total occurrences of item pairs, sorted from highest to lowest.
     - Compute the confidence for each item pair and display the results.
    """

    #Previously in Q4c, already assigned index for each transaction
    #Done using: indexed_transactions = cleaned_rdd.zipWithIndex()
    print("\n\n\nAssigned Index For Each Transaction (Displaying Only 20 Records):\n")
    print(indexed_transactions.take(20), "\n\n\n")

    #Find all possible sorted permutation of having 2 grocery items (i.e., item X and Y pair, transaction index). 
    item_permutations = indexed_transactions.flatMap(lambda x: [((tuple([x[0][i], x[0][j]]), x[1])) for i in range(len(x[0])) for j in range(len(x[0])) if i != j])
    #Then, find a list of transaction indices that are associated with a given item pair. 
    permutations_indices = item_permutations.groupByKey().mapValues(list)
    print("List of Transaction Indices Associated with a Given Permutation (Displaying Only 5 Records):\n")
    print(permutations_indices.take(5))
    print(f"\nNumber of Records: {permutations_indices.count()}\n\n\n")

    #Find the total number of times the above item pair X & Y occurs (Sort High to Low)
    permutations_counts = permutations_indices.map(lambda x: (x[0], len(x[1]))).sortBy(lambda x: -x[1])
    print("List of Number of Occurrences Associated with a Given Permutation (Sorted Highest to Lowest, Displaying Up To 20 Records):\n")
    print(permutations_counts.take(20))
    print(f"\nNumber of Records: {permutations_counts.count()}\n\n\n")

    #Now compute Frequency(X), Show total number a given X appear, Sort High to Low
    permutations_X_counts = permutations_counts.flatMap(lambda x: [(item, x[1]) for item in x[0]]).reduceByKey(lambda a, b: a + b)
    print("Computation of Frequency (X) for each given item (Sorted Highest to Lowest, Displaying Up To 20 Records):\n")
    print(permutations_X_counts.take(20))

    """
    (f) Confidence Analysis
     - Compute and display the Confidence metric, which indicates the likelihood of purchasing item Y when item X is purchased.
     - Display the top and bottom 20 item pairs sorted by their occurrence counts.
    """

    #Calculate Confidence Metrics and store it in ((item X, item Y), (occurrence count of item pair X and Y, occurrence count of item X, Confidence percentage))
    #Merging through Left Join
    transformed_permutations_counts = permutations_counts.map(lambda x: (x[0][0], x[0][1], x[1]))
    counts_dict = dict(permutations_X_counts.collect())
    joined_rdd = transformed_permutations_counts.map(lambda x: ((x[0], x[1]), (x[2], counts_dict.get(x[0], 0))))
    
    #Calculate the Confidence Metrics
    confidence_metrics = joined_rdd.map(lambda x: ((x[0], (x[1][0], x[1][1], (x[1][0] / x[1][1]) * 100))))
    print("\n\n\nConfidence Metric For Every Item Pair (Displaying Up To 20 Records):\n")
    print(confidence_metrics.take(20))
    print(f"\nNumber of Records: {confidence_metrics.count()}\n\n\n")
    
    #Find the Top TWENTY (20) item pair sorted by occurrence count of item pair X and Y, from highest to lowest.
    print("Top 20 Sorted By Occurrence Count of Item X & Occurrence Count of Item Pair:\n")
    sorted_top_confidence_metrics = confidence_metrics.sortBy(lambda x: (-x[1][1], -x[1][0]))
    top_20_by_confidence_metrics = sorted_top_confidence_metrics.take(20)
    print(top_20_by_confidence_metrics)

    #Find the Top TWENTY (20) item pair sorted by occurrence count of item pair X and Y, from highest to lowest.
    print("\n\n\nBottom 20 Sorted By Occurrence Count of Item X & Occurrence Count of Item Pair:\n")
    sorted_bottom_confidence_metrics = confidence_metrics.sortBy(lambda x: (x[1][1], x[1][0]))
    bottom_20_confidence_metrics = sorted_bottom_confidence_metrics.take(20)
    print(bottom_20_confidence_metrics)

    print("\n\n\n############### END: OUTPUT   ################\n")

    spark.stop()