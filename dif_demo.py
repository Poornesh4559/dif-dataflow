import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import logging
import argparse
def process(element):
        # Split the row by the delimiter '||' and strip any whitespace
        colm = element.split('||')
        
        # Create a dictionary to map each value to its header
        if len(colm)==70:
            data = {
            'Report_Effective_Date': colm[0],
            'Employee_ID': colm[1],
            'Worker_WID': colm[2],
            'Date_of_Birth': colm[3],
            'Gender': colm[4],
            'Home_City': colm[5],
            'Home_Postal_Code': colm[6],
            'Location_WID': colm[7],
            'Location_Code': colm[8],
            'GID': colm[9],
            'Payroll_ID': colm[10],
            'Contract_Start_Date': colm[11],
            'Contract_End_Date': colm[12],
            'Contract_Type': colm[13],
            'Employee_Status': colm[14],
            'Weekly_Schedule_Hours': colm[15],
            'Time_Type': colm[16],
            'Job_Profile_WID': colm[17],
            'Job_Profile_ID': colm[18],
            'Position_ID': colm[19],
            'Position_Title': colm[20],
            'Manager_WID': colm[21],
            'Pay_Grade': colm[22],
            'Pay_Grade_Profile': colm[23],
            'Pay_Range_Minimum': colm[24],
            'Pay_Range_Maximum': colm[25],
            'Salary_Currency': colm[26],
            'Annual_Salary': colm[27],
            'FTE': colm[28],
            'Pay_Group': colm[29],
            'Worker_Type': colm[30],
            'Worker_Sub_Type': colm[31],
            'Supervisory_Organization_WID': colm[32],
            'Supervisory_Org_Ref_ID': colm[33],
            'Cost_Center_WID': colm[34],
            'Cost_Center_Code': colm[35],
            'Hire_Date': colm[36],
            'Original_Hire_Date': colm[37],
            'Continuous_Service_Date': colm[38],
            'Is_Manager': colm[39],
            'CF_LRV_Agile_Working_on_Worker': colm[40],
            'Assignment_Status': colm[41],
            'Contract_Reason': colm[42],
            'Employing_Company': colm[43],
            'Involuntary_Termination': colm[44],
            'termination_date': colm[45],
            'termination_primary': colm[46],
            'Notional_Salary': colm[47],
            'Ready_For': colm[48],
            'Ready_Now_Assessment': colm[49],
            'Ready_Now_Completed_Rating': colm[50],
            'Ready_Now_Assessment_Date': colm[51],
            'Retention_Risk_Of_Loss': colm[52],
            'Retention_Risk_Impact_Of_Loss': colm[53],
            'Retention_Risk': colm[54],
            'Length_of_Time_Ready_Now': colm[55],
            'Regretted_Leaver': colm[56],
            'GPS_Plan_Code': colm[57],
            'GPS_Plan_Description': colm[58],
            'First_Name': colm[59],
            'Last_Name': colm[60],
            'Pension_Scheme_Indicator': colm[61],
            'DB_pension_flag_': colm[62],
            'Harmonised_Grade': colm[63],
            'Heritage_Cost_Centre_ID': colm[64],
            'LBCM_Flag': colm[65],
            'Line_Manager_First_Name': colm[66],
            'Line_Manager_Last_Name': colm[67],
            'Platform': colm[68],
            'Custom_Company': colm[69]
            }
            return data

# Define the pipeline options
options = PipelineOptions()

# Define the BigQuery schema
table_schema = {
    'fields': [
        {'name': 'Report_Effective_Date', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Employee_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Worker_WID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Date_of_Birth', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Gender', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Home_City', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Home_Postal_Code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Location_WID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Location_Code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'GID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Payroll_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Contract_Start_Date', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Contract_End_Date', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Contract_Type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Employee_Status', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Weekly_Schedule_Hours', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Time_Type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Job_Profile_WID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Job_Profile_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Position_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Position_Title', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Manager_WID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Pay_Grade', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Pay_Grade_Profile', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Pay_Range_Minimum', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Pay_Range_Maximum', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Salary_Currency', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Annual_Salary', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'FTE', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Pay_Group', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Worker_Type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Worker_Sub_Type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Supervisory_Organization_WID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Supervisory_Org_Ref_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Cost_Center_WID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Cost_Center_Code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Hire_Date', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Original_Hire_Date', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Continuous_Service_Date', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Is_Manager', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CF_LRV_Agile_Working_on_Worker', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Assignment_Status', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Contract_Reason', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Employing_Company', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Involuntary_Termination', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'termination_date', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'termination_primary', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Notional_Salary', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Ready_For', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Ready_Now_Assessment', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Ready_Now_Completed_Rating', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Ready_Now_Assessment_Date', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Retention_Risk_Of_Loss', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Retention_Risk_Impact_Of_Loss', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Retention_Risk', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Length_of_Time_Ready_Now', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Regretted_Leaver', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'GPS_Plan_Code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'GPS_Plan_Description', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'First_Name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Last_Name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Pension_Scheme_Indicator', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DB_pension_flag_', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Harmonised_Grade', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Heritage_Cost_Centre_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'LBCM_Flag', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Line_Manager_First_Name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Line_Manager_Last_Name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Platform', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Custom_Company', 'type': 'STRING', 'mode': 'NULLABLE'}
    ]
}

# Set up the Beam pipeline


def run_pipeline(argv=None):
    # Create an argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',dest='input', required=True  , help='GCS file path for input CSV file')
    parser.add_argument('--output',dest='output', required=True , help='BigQuery table in the format dataset.table')
    
    # Parse the command-line arguments
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Create PipelineOptions using the parsed arguments
    pipeline_options = PipelineOptions(pipeline_args)
    
    # Create the pipeline
    with beam.Pipeline(options=options) as p:
        (
        p
        | 'Read CSV' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
        | 'Process Rows' >> beam.Map(process)
        | 'Filter out null values' >> beam.Filter(lambda row: row is not None)
        | 'Write to BigQuery' >> WriteToBigQuery(
            table=known_args.output,
            schema = table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )
    )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
