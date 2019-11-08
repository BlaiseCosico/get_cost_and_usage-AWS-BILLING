import boto3, csv
from pprint import pprint
from datetime import datetime, timedelta



def get_cost_and_usage_data(start, end):

	ce_obj = boto3.client('ce')
	data = ce_obj.get_cost_and_usage(TimePeriod={'Start':start, 'End':end},
									 Granularity='DAILY', #MONTHY, DAILY, HOURLY
									 Metrics=['BLENDED_COST','UsageQuantity'],
									 GroupBy=[{'Type':'DIMENSION', 'Key':'SERVICE'}, {'Type':'DIMENSION', 'Key':'LINKED_ACCOUNT'}] #use LINKED_ACCOUNT to get key of account
									 )	#returns a dictionary

	return data

def export_to_csv(data):

	with open(f'bill_{now.strftime("%Y-%m-%d")}.csv', 'w+', newline='') as csvfile: #write into a dictionary file, create a new one if none exists
		fieldnames = ['TimePeriod', 'LinkedAccount', 'Service', 'Amount', 'Unit', 'Estimated'] #set column headers
		writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
		writer.writeheader()

		for results_by_time in data['ResultsByTime']: #parse data object returned by aws ce api
			for group in results_by_time['Groups']: 
					amount = group['Metrics']['BlendedCost']['Amount'] #get amount
					unit = group['Metrics']['BlendedCost']['Unit']     #get unit(USD)
					
					writer.writerow({'TimePeriod': results_by_time['TimePeriod']['Start'],  #write into csv file
									'LinkedAccount': group['Keys'][1],
									'Service': group['Keys'][0],
									'Amount': amount,
									'Unit': unit,
									'Estimated': results_by_time['Estimated']})

	return None


if __name__ == '__main__':

	now = datetime.utcnow()
	start = (now-timedelta(days=3)).strftime('%Y-%m-%d')
	end = now.strftime('%Y-%m-%d')

	data = get_cost_and_usage_data(start, end)
	export_to_csv(data)
