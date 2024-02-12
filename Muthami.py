
import tabula 
import pandas as pd
import logging
import warnings
import os
import sys
import argparse
from dateutil.relativedelta import relativedelta
warnings.filterwarnings("ignore")


def extract_and_condense_data(path):
    # path to the file
    pdf_path = path 
    # list of all expeted columns 
    col_list=['Date', 'Value date', 'Description', 'Debit', 'Credit', 'Balance']
    #pages to be extracted
    page = "all"
    data= tabula.read_pdf(pdf_path, pages=page)
    dfs=[]
    clean_dfs=[]
    for table in data:
        #Remove the "unnmaed" feature 
        unnamed = [f"Unnamed: {i}" for i in range(0, len(data) + 1)]
    
        for col_name in unnamed:
            if col_name in table.columns:
                table['Balance'] = table[col_name]  
                table.drop(columns=[col_name], inplace=True)
        
        
    for table in data:
        # Check if table has required features
    
            if isinstance(table, pd.DataFrame) and not table.empty:
                if len(table.columns) == len(col_list):
                    clean_dfs.append(table)
                else:
                    dfs.append(table)
               
    #Set columns in eact table to be the required ones
    for table in clean_dfs:
        table.columns = col_list
    # Combine all extracted tables
    final_df = pd.concat(clean_dfs, ignore_index=True)
     
      
    # logging.info(f"Number of tables extracted : {len(data)}")
    # logging.info(f"Number of Unneeded tables : {len(dfs)}") 
    # logging.info(f"Number Needed tables: {len(clean_dfs)}")
    # logging.info(f"Shape of final_data_frame : {final_df.shape}")
    
    print(f"Number of tables extracted : {len(data)}")
    print(f"Number of Unneeded tables : {len(dfs)}") 
    print(f"Number Needed tables: {len(clean_dfs)}")
    print(f"Shape of final_data_frame : {final_df.shape}")
     
    return final_df


def clean_and_process_data(final_df):
    # Null vallues
    
    print(f'Null vallues per field  :\n {final_df.isnull().sum()}')
    #drop rows where "Date" column is null
    final_df.dropna(subset=['Date'], inplace=True)
    
    #type conversion
    numerical_cols=['Debit', 'Credit', 'Balance']
    for col in numerical_cols:
        final_df[col] = final_df[col].str.replace(',', '').astype(float)
    
    #Date column conversions
    date_cols=['Date','Value date']
    for col in date_cols:
        final_df[col] = pd.to_datetime(final_df[col], format="%d/%m/%Y")
    
    return final_df

    

# def aggs(final_df):
#     # final_df=clean_and_process_data(final_df)
#     max_date=final_df['Date'].max()
#     last_6_months = final_df[final_df['Date'] >= max_date + relativedelta(
#             months=-6)]
#     last_3_months = final_df[final_df['Date'] >= max_date + relativedelta(
#             months=-3)]
#     Frame={"Time_Space":["Whole_time","last_6_months","last_3_months"],
#       "Total_Debit":[final_df["Debit"].sum(),last_6_months["Debit"].sum(),last_3_months["Debit"].sum()],
#       "Average_Debit":[final_df["Debit"].mean(),last_6_months["Debit"].mean(),last_3_months["Debit"].mean()],
#       "Max_Debit":[final_df["Debit"].max(),last_6_months["Debit"].max(),last_3_months["Debit"].max()],
#       "Min_Debit":[final_df["Debit"].min(),last_6_months["Debit"].min(),last_3_months["Debit"].min()],
#       "Total_Credit":[final_df["Credit"].sum(),last_6_months["Credit"].sum(),last_3_months["Credit"].sum()],
#        "Average_Credit":[final_df["Credit"].mean(),last_6_months["Credit"].mean(),last_3_months["Credit"].mean()],
#       "Max_Credit":[final_df["Credit"].max(),last_6_months["Credit"].max(),last_3_months["Credit"].max()],
#       "Min_Credit":[final_df["Credit"].min(),last_6_months["Credit"].min(),last_3_months["Credit"].min()],
#       "Total_Balance":[final_df["Balance"].sum(),last_6_months["Balance"].sum(),last_3_months["Balance"].sum()],
#        "Average_Balance":[final_df["Balance"].mean(),last_6_months["Balance"].mean(),last_3_months["Balance"].mean()],
#       "Max_Balance":[final_df["Balance"].max(),last_6_months["Balance"].max(),last_3_months["Balance"].max()],
#       "Min_Balance":[final_df["Balance"].min(),last_6_months["Balance"].min(),last_3_months["Balance"].min()]}
#     Ags_output=pd.DataFrame(Frame)
    
#     print(f"last_6_month min-date : {last_6_months['Date'].min()}")
#     print(f"last_6_month max-date : {last_6_months['Date'].max()}")
#     print(f"last_3_month min-date : {last_3_months['Date'].min()}")
#     print(f"last_3_month max-date : {last_3_months['Date'].max()}") 
#     print(f"whole_time max-date : {final_df['Date'].max()}") 
#     print(f"whole_time min-date : {final_df['Date'].min()}") 
#     return Ags_output


# In[427]:

def aggs(final_df):
    # Convert 'Date' column to datetime and create 'Transaction_Month' column
    final_df['Date'] = pd.to_datetime(final_df['Date'])
    final_df['Transaction_Month'] = final_df['Date'].dt.strftime('%b-%y')

    # Group by 'Transaction_Month' and calculate aggregates
    Year_month_summaries = final_df.groupby(['Transaction_Month']).agg(
        debit_count=pd.NamedAgg('Debit', 'count'), 
        Total_debit=pd.NamedAgg('Debit', 'sum'),
        credit_count=pd.NamedAgg('Credit', 'count'),
        Total_credit=pd.NamedAgg('Credit', 'sum'),
        Max_balance=pd.NamedAgg('Balance', 'max'),
        Min_balance=pd.NamedAgg('Balance', 'min')
    ).reset_index()

    # Sort the DataFrame by 'Transaction_Month'
    
    Year_month_summaries = Year_month_summaries.sort_values(by='Transaction_Month', ascending=True, key=lambda x: pd.to_datetime(x, format='%b-%y'))

    # Calculate sum and average rows
    sum_row = Year_month_summaries.drop(columns=['Transaction_Month', 'Max_balance', 'Min_balance']).sum()
    average_row = Year_month_summaries.drop(columns=['Transaction_Month', 'Max_balance', 'Min_balance']).mean()

    # Create total row
    total_row = pd.DataFrame(sum_row).T
    total_row['Transaction_Month'] = 'Total'
    total_row['Max_balance'] = ''  # Leave 'Max_balance' blank
    total_row['Min_balance'] = ''  # Leave 'Min_balance' blank

    # Create average row
    average_row = pd.DataFrame(average_row).T
    average_row['Transaction_Month'] = 'Average'
    average_row['Max_balance'] = ''  # Leave 'Max_balance' blank
    average_row['Min_balance'] = ''  # Leave 'Min_balance' blank

    # Append total and average rows to the DataFrame
    Year_month_summaries = pd.concat([Year_month_summaries, total_row, average_row], ignore_index=True)

    # Apply styling to the DataFrame
    Year_month_summaries_styled = Year_month_summaries.style.apply(lambda row: ['font-weight: bold' if row['Transaction_Month'] in ['Total', 'Average'] else '' for _ in row], axis=1)
    
    return Year_month_summaries_styled





# def out_put(path, client_name,cleaned_csv_filename="Cleaned_and_transformed_data", aggregated_csv_filename="summerised_aggregates"):
#     try:
#         # Determine root directory
#         root_dir = os.getcwd()
    
        
#         # Join root directory with file names
#         cleaned_csv_path = os.path.join(root_dir, client_name+"_"+cleaned_csv_filename + ".csv")
#         aggregated_csv_path = os.path.join(root_dir, client_name+"_"+aggregated_csv_filename + ".csv")
        
#         # Perform data processing
#         final_df = extract_and_condense_data(path)
#         final_df = clean_and_process_data(final_df)
        
#         # Save cleaned data to CSV
#         final_df.to_csv(cleaned_csv_path, index=False)  # Set index=False to avoid saving row numbers
        
        
#         # Aggregate data and save to CSV
#         final_df = aggs(final_df)
#         final_df.to_csv(aggregated_csv_path, index=False)  # Set index=False to avoid saving row numbers
#         print(f'Navigate to {cleaned_csv_path} to access Cleaned_and_transformed_data csv')
#         print(f'Navigate to {aggregated_csv_path} to access summerised_aggregates_data csv')
#         return final_df
#     except Exception as e:
#         print(e)



def out_put(path, client_name, cleaned_csv_filename="Cleaned_and_transformed_data", aggregated_csv_filename="summerised_aggregates"):
    try:
        # Determine root directory
        root_dir = os.getcwd()
    
        # Join root directory with file names
        cleaned_csv_path = os.path.join(root_dir, client_name + "_" + cleaned_csv_filename + ".csv")
        aggregated_csv_path = os.path.join(root_dir, client_name + "_" + aggregated_csv_filename + ".csv")
        
        # Perform data processing
        final_df = extract_and_condense_data(path)
        final_df = clean_and_process_data(final_df)
        
        # Save cleaned data to CSV
        final_df.to_csv(cleaned_csv_path, index=False)  # Set index=False to avoid saving row numbers
        
        # Aggregate data and save to CSV
        final_df_aggregated = aggs(final_df)  # Call the aggs function to aggregate data and style the DataFrame
        final_df_aggregated_df = final_df_aggregated.data  # Convert Styler object to DataFrame
        final_df_aggregated_df.to_csv(aggregated_csv_path, index=False)  # Save DataFrame to CSV
        
        print(f'Navigate to {cleaned_csv_path} to access Cleaned_and_transformed_data csv')
        print(f'Navigate to {aggregated_csv_path} to access summerised_aggregates_data csv')
        return final_df_aggregated
    except Exception as e:
        print(e)





# result=out_put(path="axle capitol ltd 22.9.pdf",client_name="DTB")
# result
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process data for a client.")
    parser.add_argument("client_name", type=str, help="Name of the client")
    parser.add_argument("path", type=str, help="Path to the data file")
    args = parser.parse_args()
    
    out_put(args.path, args.client_name)


