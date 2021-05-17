import psycopg2
import sys, os
import numpy as np
import pandas as pd
import pandas.io.sql as psql
from datetime import datetime
import csv
from flask import Flask, jsonify,request
app = Flask(__name__)




PGHOST='localhost'
PGDATABASE='pgdatabase'
PGUSER='postgres'
PGPASSWORD='password'

class Connection: 
    def _init_(self):
        self._conn = psycopg2.connect(database=PGDATABASE, user=PGUSER,
                                      password=PGPASSWORD, host=PGHOST,
                                      port=5432)

    def get_connection(self):
        return self._conn



@app.route('/createReport', methods=["GET","POST"])
def createReport():
    if request.method == "POST":
        conn = psycopg2.connect(database=PGDATABASE, user=PGUSER,
                                      password=PGPASSWORD, host=PGHOST,
                                      port=5432)

        cur = conn.cursor()
        data = pd.read_csv(r'Flipkart_ Mar_ 2021_Sales_and_Returns.csv', names=['Seller_GSTIN', 'Order_ID', 'Order_Item_ID',
       'Product_Title', 'FSN', 'SKU', 'HSN_Code', 'Event_Type',
       'Event_Sub_Type', 'Order_Type', 'Fulfilment_Type', 'Order_Date',
       'Order_Approval_Date', 'Item_Quantity', 'Order_Shipped_From',
       'Price_before_discount', 'Total_Discount', 'Seller_Share',
       'Bank_Offer_Share',
       'Price_after_discount',
       'Shipping_Charges',
       'Final_Invoice_Amount',
       'Type_of_tax', 'Taxable_Value',
       'CST_Rate', 'CST_Amount', 'VAT_Rate', 'VAT_Amount', 'Luxury_Cess_Rate',
       'Luxury_Cess_Amount', 'IGST_Rate', 'IGST_Amount', 'CGST_Rate',
       'CGST_Amount', 'SGST_Rate',
       'SGST_Amount', 'TCS_IGST_Rate',
       'TCS_IGST_Amount', 'TCS_CGST_Rate', 'TCS_CGST_Amount', 'TCS_SGST_Rate',
       'TCS_SGST_Amount', 'Total_TCS_Deducted', 'Buyer_Invoice_ID',
       'Buyer_Invoice_Date', 'Buyer_Invoice_Amount',
       "Customers_Billing_Pincode", "Customers_Billing_State",
       "Customers_Delivery_Pincode", "Customers_Delivery_State",
       'Usual_Price'])
        df = pd.DataFrame(data, columns=['Seller_GSTIN', 'Order_ID', 'Order_Item_ID',
       'Product_Title', 'FSN', 'SKU', 'HSN_Code', 'Event_Type',
       'Event_Sub_Type', 'Order_Type', 'Fulfilment_Type', 'Order_Date',
       'Order_Approval_Date', 'Item_Quantity', 'Order_Shipped_From',
       'Price_before_discount', 'Total_Discount', 'Seller_Share',
       'Bank_Offer_Share',
       'Price_after_discount',
       'Shipping_Charges',
       'Final_Invoice_Amount',
       'Type_of_tax', 'Taxable_Value',
       'CST_Rate', 'CST_Amount', 'VAT_Rate', 'VAT_Amount', 'Luxury_Cess_Rate',
       'Luxury_Cess_Amount', 'IGST_Rate', 'IGST_Amount', 'CGST_Rate',
       'CGST_Amount', 'SGST_Rate',
       'SGST_Amount', 'TCS_IGST_Rate',
       'TCS_IGST_Amount', 'TCS_CGST_Rate', 'TCS_CGST_Amount', 'TCS_SGST_Rate',
       'TCS_SGST_Amount', 'Total_TCS_Deducted', 'Buyer_Invoice_ID',
       'Buyer_Invoice_Date', 'Buyer_Invoice_Amount',
       "Customers_Billing_Pincode", "Customers_Billing_State",
       "Customers_Delivery_Pincode", "Customers_Delivery_State",
       'Usual_Price'] )
    
        query = """ INSERT INTO flipkart_sales_and_returns_mar_2021 (seller_gst_in,order_id,order_item_id,product_title_description,fsn,sku,hsn_code,event_type,event_sub_type,order_type,fulfilment_type,order_date,order_approval_date,item_quantity,order_shipped_from,price_before_dicount,total_dicount,seller_share,bank_offer_share,price_after_discount,shipping_charges,final_invoice_amount,type_of_tax,taxable_value,cst_rate,cst_amount,vat_rate,vat_amount,luxury_cess_rate,luxury_cess_amount,igst_rate,igst_amount,cgst_rate,cgst_amount,sgst_rate,sgst_amount,tcs_igst_rate,tcs_igst_amount,tcs_cgst_rate,tcs_cgst_amount,tcs_sgst_rate,tcs_sgst_amount,total_tcs_deducted,buyer_invoice_id,buyer_invoice_date,buyer_invoice_amount,customer_billing_pin,customer_billing_state,customer_delivery_pin,customer_delievery_state,usual_price) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        try:
            for row in df.itertuples():
                rec = (row.Seller_GSTIN, row.Order_ID, row.Order_Item_ID, row.Product_Title, row.FSN, row.SKU, row.HSN_Code, row.Event_Type,
                    row.Event_Sub_Type, row.Order_Type, row.Fulfilment_Type, row.Order_Date, row.Order_Approval_Date, row.Item_Quantity,row.Order_Shipped_From,
                    row.Price_before_discount, row.Total_Discount, row.Seller_Share, row.Bank_Offer_Share, row.Price_after_discount,
                    row.Shipping_Charges, row.Final_Invoice_Amount, row.Type_of_tax, row.Taxable_Value, row.CST_Rate, row.CST_Amount, row.VAT_Rate,
                    row.VAT_Amount, row.Luxury_Cess_Rate, row.Luxury_Cess_Amount, row.IGST_Rate, row.IGST_Amount, row.CGST_Rate, row.CGST_Amount,
                    row.SGST_Rate, row.SGST_Amount, row.TCS_IGST_Rate, row.TCS_IGST_Amount, row.TCS_CGST_Rate, row.TCS_CGST_Amount, row.TCS_SGST_Rate,
                    row.TCS_SGST_Amount, row.Total_TCS_Deducted, row.Buyer_Invoice_ID, row.Buyer_Invoice_Date, row.Buyer_Invoice_Amount, row.Customers_Billing_Pincode,
                    row.Customers_Billing_State, row.Customers_Delivery_Pincode, row.Customers_Delivery_State, row.Usual_Price)
                cur.execute(query, rec)
                conn.commit()
            print("record inserted successfull")

        except (Exception, psycopg2.Error) as error:
            print("Failed to insert record into table", error)
        return "success"


# class QueryExecutor:
#     def _init_(self, query_string, args, single_result=None):
#         self._query_string = query_string
#         self._args = args
#         self._single_result = single_result

#     def execute(self):
#         conn = Connection().get_connection()
#         cur = conn.cursor()
#         rows = None
#         try:
#             cur.execute(self._query_string, self._args)
#             conn.commit()
#             if self._single_result is not None:
#                 rows = cur.fetchone() if self._single_result else cur.fetchall()

#             return rows

#         except:
#             logger.exception('SQL Exception occurred :: %s' % cur.query)
#             traceback.print_exc()
#             conn.rollback()
#         finally:
#             cur.close()
#             conn.close()

# ### getting all the entries for all event_sub_type in a date range 

# # sales_and_return = input('Enter tne name of the sales and returns file in csv format and underscore in it')
# # receipt = input('Enter the name of the receipt file in csv format and underscore in it')
# # to_date = input('to date in the format yyyy-mm-dd')
# # from_date = input('from date in the format yyyy-mm-dd')




# def get_table(to_date,from_date):
#     query_string = " select * from flipkart_sales_and_returns_mar_2021 " \
#                     " where (event_sub_type = 'Sale' or event_sub_type = 'Return Cancellation') " \
#                     " and buyer_invoice_date >= %s and buyer_invoice_date <= %s "
#     args = (to_date,from_date,)
#     sale_table = QueryExecutor(query_string, args, False).execute()

#     query_string = "select * from flipkart_sales_and_returns_mar_2021 where event_sub_type = 'Return' and buyer_invoice_date >= %s "
#     args = (to_date,)
#     return_table = QueryExecutor(query_string, args, False).execute()

#     with open('flipkart_sales_and_returns_with_date.csv','w') as date:
#         writer = csv.writer(date)
#         for row in sale_table:
#             writer.writerow(row)
#         for rw in return_table:
#             writer.writerow(rw)


# ### getting the name of products and their gst rate and the sku. 
# def get_tally_name_and_gst():
#     query_string = "select name_as_per_tally , gst_rate, Flipkart_SKU from flipkart_tally_name_and_sku"
#     args = None
#     name_and_gst = QueryExecutor(query_string, args, False).execute()
#     return name_and_gst


# ### getting all the cancellation records from a starting date till the date of entries available.
# def get_order_id_and_order_item_id_for_cancellation(to_date):
#     query_string = "select * from flipkart_sales_and_returns_mar_2021 where Event_Sub_Type = 'Cancellation' and buyer_invoice_date >= %s " 
#     args = (to_date,)
#     order_id_and_order_item_id = QueryExecutor(query_string, args, False).execute()
#     return order_id_and_order_item_id


# ### getting the records with the order_id and order_item_id of cancellation records in the date range
# def sales_records_against_cancellation_order(order_id,order_item_id,to_date,from_date):
#     query_string = " select * from flipkart_sales_and_returns_mar_2021 " \
#                     " where (Event_Sub_Type = 'Sale' or Event_Sub_Type = 'Return Cancellation') and " \
#                     " buyer_invoice_date >= %s " \
#                     " and buyer_invoice_date <= %s " \
#                     " and order_id = %s and order_item_id = %s "
#     args = (order_id, order_item_id,to_date,from_date,)
#     records = QueryExecutor(query_string,args,False).execute()
#     if records:
#         return records[0]


# ### all the records in a csv file with sales and cancellation records
# def get_sales_record_against_cancellation_orders():
#     cancellation_orders = get_order_id_and_order_item_id_for_cancellation()
#     with open ('Flipkart_sales_and_cancellation_rows.csv','w') as cancel:
#         writer = csv.writer(cancel)
#         for order in cancellation_orders:
#             writer.writerow(order)
#             rows = sales_records_against_cancellation_order(order[1],order[2]) 
#             if rows is None:
#                 a=0
#             else:
#                 writer.writerow(rows)
#     # print('cancellation rows')



# ### getting the records on which the actual report file has to be generated. 
# ###So this gives the records on for which the analysis has to be done

# def sales_and_returns_entries():
#     with open('flipkart_sales_and_returns_with_date.csv','r') as date, open('Flipkart_sales_and_cancellation_rows.csv','r') as cancel, open('Flipkart_Sales_and_Returns_without_cancellation.csv','w') as report:
#         writer = csv.writer(report)
#         cancel = [i for i in csv.reader(cancel)]
#         for i in csv.reader(date):
#             if i not in cancel:
#                 writer.writerow(i)
#     # print('without cancellation')



# def sales_quantity():
#     dic = {}
#     new_dic = {}
#     data = pd.read_csv('Flipkart_Sales_and_Returns_without_cancellation.csv')
#     x = np.array(data.loc[:].values)
#     lst = x.tolist()
#     for i in lst:
#         if (i[8] == 'Sale') or (i[8] == 'Return Cancellation'):
#             if i[5] not in dic:
#                 dic[i[5]] =[1]
#             else:
#                 dic[i[5]][0] += 1
#             dic[i[5]].append((i[1]+str(i[2])))
#     for i in get_tally_name_and_gst():
#         for k, v in dic.items():
#             if k[3:-3] == i[2]:
#                 new_dic[i[0]] = dic[k]

#     return new_dic

# # sales_quantity()


# def return_query(id):
#     query_string = " select * from flipkart_sales_and_returns_mar_2021 " \
#                     " where concat(order_id, order_item_id) = %s " \
#                     " and event_sub_type = 'Return' "
#     args = (id,)
#     records = QueryExecutor(query_string,args,False).execute()
#     if records:
#         return records[0]
#     else:
#         return 0


# def return_quantity():
#     sales = sales_quantity()
#     actual_returns = {}
#     for k, v in sales.items():
#         if k not in actual_returns:
#             actual_returns[k] = []
#         if len(v) > 0:
#             for i in v[1:]:
#                 return_record = return_query(i)
#                 if return_record:
#                     actual_returns[k].append((return_record[1]+str(return_record[2])))

#     return actual_returns
# # return_quantity()

# def sales_minus_returns():
#     sales_dic = sales_quantity()
#     returns_dic = return_quantity()
#     diff_dic = {}
#     for key,value in returns_dic.items():
#         if key in sales_dic:
#             diff_dic[key] = list(set(sales_dic[key][1:]) - set(value[:]))

#     return diff_dic


# def all_query(id):
#     query_string = " select settlement_value, total_offer_amount, my_share, commission, Collection_Fee, " \
#                     " Fixed_Fee, Shipping_Fee, Reverse_Shipping_Fee, Tax_collected_At_Source, TDS, Taxes, Protection_Fund" \
#                     " from flipkart_receipts_march_2021 " \
#                     " where concat(order_id, order_item_id) =  %s "
#     args = (id,)
#     records = QueryExecutor(query_string,args,False).execute()
#     if len(records) > 0:
#         settlement_value = 0
#         total_offer_amount = 0
#         my_share = 0
#         commission = 0
#         Collection_Fee = 0
#         Fixed_Fee = 0
#         Shipping_Fee = 0
#         Reverse_Shipping_Fee = 0
#         Tax_collected_At_Source = 0
#         TDS = 0
#         Taxes = 0
#         Protection_Fund = 0
#         for record in records:    
#             settlement_value = settlement_value + record[0]
#             total_offer_amount = total_offer_amount + record[1]
#             my_share = my_share + record[2]
#             commission = commission + record[3]
#             Collection_Fee = Collection_Fee + record[4]
#             Fixed_Fee = Fixed_Fee + record[5]
#             Shipping_Fee = Shipping_Fee + record[5]
#             Reverse_Shipping_Fee = Reverse_Shipping_Fee + record[6]
#             Tax_collected_At_Source = Tax_collected_At_Source + record[7]
#             TDS = TDS + record[8]
#             Taxes = Taxes + record[9]
#             Protection_Fund = Protection_Fund + record[10]
#         return settlement_value, total_offer_amount, my_share, commission,Collection_Fee, Fixed_Fee, Shipping_Fee, Reverse_Shipping_Fee, Tax_collected_At_Source, TDS, Taxes, Protection_Fund
#     else:
#         return 0,0,0,0,0,0,0,0,0,0,0,0


# def all_values():
#     dic = sales_quantity()
#     for k,v in dic.items():
#         if len(v) > 0:
#             settlement_value = 0
#             total_offer_amount = 0
#             my_share = 0
#             commission = 0
#             Collection_Fee = 0
#             Fixed_Fee = 0
#             Shipping_Fee = 0
#             Reverse_Shipping_Fee = 0
#             Tax_collected_At_Source = 0
#             TDS = 0
#             Taxes = 0
#             Protection_Fund = 0
#             for i in v[1:]:
#                 record = all_query(i)
#                 settlement_value = settlement_value +  record[0]
#                 total_offer_amount = total_offer_amount + record[1]
#                 my_share = my_share + record[2]
#                 commission = commission + record[3]
#                 Collection_Fee = Collection_Fee + record[4]
#                 Fixed_Fee = Fixed_Fee + record[5]
#                 Shipping_Fee = Shipping_Fee + record[5]
#                 Reverse_Shipping_Fee = Reverse_Shipping_Fee + record[6]
#                 Tax_collected_At_Source =  Tax_collected_At_Source + record[7]
#                 TDS = TDS + record[8]
#                 Taxes = Taxes + record[9]
#                 Protection_Fund = Protection_Fund + record[10]
#             v.append(settlement_value)
#             v.append(total_offer_amount)
#             v.append(my_share)
#             v.append(commission)
#             v.append(Collection_Fee)
#             v.append(Fixed_Fee)
#             v.append(Shipping_Fee)
#             v.append(Reverse_Shipping_Fee)
#             v.append(Tax_collected_At_Source)
#             v.append(TDS)
#             v.append(Taxes)
#             v.append(Protection_Fund)
#         # print(v)
#     return dic
# # print(all_values())


# def values():
#     dic = all_values()
#     final_values ={}
#     for k,v in dic.items():
#         final_values[k] = [v[0],v[-8],v[-7],v[-6],v[-5],v[-4],v[-3],v[-2],v[-1]]
#     return final_values
# # print(values())



# def taxable_value_query(id):
    
#     query_string = " select taxable_value_final_invoice_amount_taxes from flipkart_sales_and_returns_mar_2021 " \
#                     " where concat(order_id, order_item_id) = %s "
#     args = (id,)
#     records = QueryExecutor(query_string,args,False).execute()
#     if len(records) > 0:
#         total = 0
#         for record in records:    
#             total = total + record[0]
#         return total
#     else:
#         return 0

# def taxable_value():
#     dic = sales_minus_returns()
#     for k,v in dic.items():
#         total = 0
#         for i in v[:]:
#             total = total + taxable_value_query(i)
#         v.append(total)
#     return dic
# # print(taxable_value())


# def final_invoice_amount_query(id):
#     query_string = " select final_invoice_amount_price_after_discount_plus_shipping_charges " \
#                     " from flipkart_sales_and_returns_mar_2021 " \
#                     " where concat(order_id,order_item_id) = %s and " \
#                     " (event_sub_type = 'Sale' or event_sub_type = 'Return Cancellation') "
#     args = (id,)
#     records = QueryExecutor(query_string,args,False).execute()
#     if len(records) > 0:
#         return records[0][0]
#     else:
#         return 0
    


# def final_invoice_amount():
#     dic = sales_minus_returns()
#     for k,v in dic.items():
#         total = 0
#         for i in v[:]:
#             total = total + int(final_invoice_amount_query(i))
#         v.append(total)
#     # for k,v in dic.items():
#     #     print(k,v[-1])
#     return dic
# # print(final_invoice_amount())



# def finals():

#     final_values = {}
#     for k,v in sales_quantity().items():
#         final_values[k] = [v[0]]
#     print('sales done')
#     for k,v in return_quantity().items():
#         if k in final_values:
#             final_values[k].append(len(v))
#     print('returns done')
#     for k,v in sales_minus_returns().items():
#         final_values[k].append(len(v))
#     print('net quantity done')
#     for k,v in final_invoice_amount().items():
#         final_values[k].append(v[-1])
#     print('final invoice done')
#     for k,v in taxable_value().items():
#         final_values[k].append(v[-1])
#     print('taxable value done')
#     for k,v in all_values().items():
#         final_values[k].append(v[-12])
#         final_values[k].append(v[-11])
#         final_values[k].append(v[-10])
#         final_values[k].append(v[-9])
#         final_values[k].append(v[-8])
#         final_values[k].append(v[-7])
#         final_values[k].append(v[-6])
#         final_values[k].append(v[-5])
#         final_values[k].append(v[-4])
#         final_values[k].append(v[-3])
#         final_values[k].append(v[-2])
#         final_values[k].append(v[-1])
#     return final_values

# # finals()

# def online_price_of_item(item_name):
#     query_string = " select online_price_2020_21 from flipkart_tally_name_and_sku " \
#                     " where name_as_per_tally = %s "
#     args = (item_name,)
#     records = QueryExecutor(query_string,args,False).execute()
#     if records:
#         return records[0][0]
#     else:
#         return 1

# @app.route('/generateReport')

# def writing():
#     to_date= datetime.strptime('2021-03-12', '%Y-%m-%d')
#     from_date= datetime.strptime('2021-03-01', '%Y-%m-%d')
#     get_table(to_date,from_date)
#     get_sales_record_against_cancellation_orders()
#     sales_and_returns_entries()
#     with open('report_result2.csv','w') as final_report:
#         writer = csv.writer(final_report)
#         heading = ['Item Name','Sales Quantity','Return Quantity','Net Quantity',
#                     'Final Invoice Amount','Taxable Value','Settlement Value','Total Offer Amount',
#                     'My Share','Commission','Collection Fee','Fixed Fee','Shipping Fee','Reverse Shipping Fee',
#                     'Tax Collected At Source','TDS','Taxes','Protection Fund','Total Expenses','Total Expenses Rate On Taxable Value',
#                     'Return Quantity Rate On Sales Quantity','Net Settlement','Per Piece Net Settlement','Fc per Piece',
#                     'Per pc Gain or Loss','Per pc Gain or Loss Percentage','Total Gain Loss','Average Selling Price']

#         writer.writerow(heading)

#         lst = []
        
#         for k,v in finals().items():
#             lst_2 = []
#             lst_2.append(k)
#             for i in v:
#                 lst_2.append(i)

#             total_expenses = sum(v[6:13])+ v[-1]
#             if v[4] == 0:
#                 total_expenses_rate_on_taxable_value = str((total_expenses/1) * 100)+'%'
#             else:
#                 total_expenses_rate_on_taxable_value = str((total_expenses/v[4]) * 100)+'%'
#             if v[2] == 0:
#                 return_quantity_rate_on_sales_quantity = str(v[1]/1 * 100) + '%'
#             else: 
#                 return_quantity_rate_on_sales_quantity = str(v[1]/v[0] * 100 ) + '%' 
#             net_settlement = (v[5]-(v[3] - v[4]) + v[-4] + v[-2] + v[-3])
#             if v[2] == 0:
#                 per_piece_net_settlement = (net_settlement/1)
#             else:
#                 per_piece_net_settlement = (net_settlement/v[2])
#             fc_per_pc = online_price_of_item(k)
#             per_pc_gain_or_loss = (per_piece_net_settlement - fc_per_pc)
#             per_pc_gain_or_loss_percentage = str((per_pc_gain_or_loss/ fc_per_pc) * 100) + '%'
#             total_gain_loss = per_pc_gain_or_loss * v[2]
#             if v[2] == 0:
#                 average_selling_price = v[3]/1
#             else:
#                 average_selling_price = v[3]/v[2]


#             lst_2.append(total_expenses)
#             lst_2.append(total_expenses_rate_on_taxable_value)
#             lst_2.append(return_quantity_rate_on_sales_quantity)
#             lst_2.append(net_settlement)
#             lst_2.append(per_piece_net_settlement)
#             lst_2.append(fc_per_pc)
#             lst_2.append(per_pc_gain_or_loss)
#             lst_2.append(per_pc_gain_or_loss_percentage)
#             lst_2.append(total_gain_loss)
#             lst_2.append(average_selling_price)
#             lst.append(lst_2)
#         for i in lst:
#             writer.writerow(i)

#     #return jsonify(final_report)
# writing()
# if _name_ == "_main_":
#     app.run()
# # total_expenses = total_offer_amount upto Reverse_Shipping_Fee + Protection_Fund (except Tax_collected_At_Source and taxes)

# # total_expenses_rate_on_taxable_value = (total_expenses/taxable_value * 100) % symbol should be addded

# # return_quantity_rate_on_sales_quantity = (return_quantity/sales_quantity) % symbol should be addded

# # net_settlement(less_gst_add_tcs) =  (settlement_value - (final_invoice_amount - taxable_value) + Tax_collected_At_Source + Taxes)

# # per_piece_net_settlement = (net_settlement/net_quantity)

# # fc_per_pc = onlinceprice from flipkart_tally_name_and_sku table

# # per_pc_gain_or_loss = (per_piece_net_settlement - fc_per_pc)

# # per_pc_gain_or_loss_percentage = (per_pc_gain_or_loss / fc_per_pc * 100) % symbol should be addded

# # total_gain_loss = per_pc_gain_or_loss * net_quantity

# # average_selling_price(inclusive_of_tax) = final_invoice_amount/net_quantity