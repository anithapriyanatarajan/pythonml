#Customer Analytics

import numpy as np
import pandas as pd

def main():

    dfcust_src = pd.read_csv("input/Relevency_table.csv")
    dfprod_src = pd.read_csv("input/Products.csv")
    dfexcl_src = pd.read_csv("input/Exclusion.csv")

    dfcust_tmp1 = dfcust_src.copy()
    dfprod_tmp1 = dfprod_src.copy()
    dfexcl_tmp1 = dfexcl_src.copy()

    ### stage 1 - Group, Sort and Eliminate customer records who would qualify for < 3products.

    dfcust_grped = dfcust_tmp1.groupby('product')
    print(dfcust_tmp1.shape)


    #Create an Empty dataframe to store first iteration datasets
    dfcust_stage1 = pd.DataFrame(columns = ['customers', 'product', 'relevancy_score']) 
    for index, row in dfprod_tmp1.iterrows():
          prodind = index
          prodid = row['product']
          prodvolume = row['volume']
          prodg = dfcust_grped.get_group(prodid)
          prodg = prodg.sort_values(by = 'relevancy_score', ascending=False)
          prodgsliced = prodg.iloc[:prodvolume]
          dfcust_stage1 = dfcust_stage1.append(prodgsliced)

    #Identify customer records whose product count is less than 3
    dfcust_elist1 = pd.DataFrame(dfcust_stage1.groupby(['customers']).product.count()[lambda x: x<3])
  
    #Drop customer records which have better relevancy score but count < 3
    for index, row in dfcust_elist1.iterrows(): 
          elicust = index
          dfcust_tmp1 = dfcust_tmp1[dfcust_tmp1.customers != elicust]

    print(dfcust_tmp1.shape)

    #Stage 2: Eliminate mutually exclusive records
    # Copy Stage 1 output dataframe to a new dataframe for stage processing
 
    dfcust_tmp2 = dfcust_tmp1.copy()

    dfcust_grped2 = dfcust_tmp2.groupby('product')
    print(dfcust_tmp2.shape)


    #Create an Empty dataframe to store first iteration datasets
    dfcust_stage2 = pd.DataFrame(columns = ['customers', 'product', 'relevancy_score'])
    for index, row in dfprod_tmp1.iterrows():
          prodind2 = index
          prodid2 = row['product']
          prodvolume2 = row['volume']
          prodg2 = dfcust_grped2.get_group(prodid2)
          prodg2 = prodg2.sort_values(by = 'relevancy_score', ascending=False)
          prodgsliced2 = prodg2.iloc[:prodvolume2]
          dfcust_stage2 =  dfcust_stage2.append(prodgsliced2)

    #Apply Mutual Exclusion and Max count per customer 
    #Group the sorted dfcust_stage2 dataframe by product. Create a new dataframe that holds the product name and sum of relevancy score
    dfprod_maxscore = pd.DataFrame(columns = ['product', 'sum_scores'])
    dfprod_score = dfcust_stage2.groupby('product').sum()
    print(dfprod_score)
    for index, row in dfprod_tmp1.iterrows():
          prodind3 = index
          prodid3 = row['product']
          scoresum = dfprod_score.loc[dfprod_score.index == prodid3, dfprod_score['relevancy_score'].iloc[0]
          proddf = pd.Dataframe([[prodid3,scoresum]])
          dfprod_maxscore =  dfprod_maxscore.append(proddf)
          print(dfprod_maxscore)


     

main()
