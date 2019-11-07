import boto3, csv
from pprint import pprint
from datetime import datetime, timedelta


now = datetime.utcnow()
start = (now-timedelta(days=3)).strftime('%Y-%m-%d')
end = now.strftime('%Y-%m-%d')

ce_obj = boto3.client('ce')

results = []


data = ce_obj.get_cost_and_usage(TimePeriod={'Start':start, 'End':end},
								 Granularity='DAILY',
								 Metrics=['BLENDED_COST','UsageQuantity'],
								 GroupBy=[{'Type':'DIMENSION', 'Key':'SERVICE'}, {'Type':'DIMENSION', 'Key':'LINKED_ACCOUNT'}] #use LINKED_ACCOUNT to get key of account
								 )	#returns a dictionary

with open('bill.csv', 'w+', newline='') as csvfile: #write into a dictionary file, create a new one if none exists
	fieldnames = ['TimePeriod', 'LinkedAccount', 'Service', 'Amount', 'Unit', 'Estimated'] #set column headers
	writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
	writer.writeheader()
	for results_by_time in data['ResultsByTime']: #parse data object returned by ce api
		for group in results_by_time['Groups']:
				amount = group['Metrics']['BlendedCost']['Amount'] #get amount
				unit = group['Metrics']['BlendedCost']['Unit']     #get unit(USD)
				
				writer.writerow({'TimePeriod': results_by_time['TimePeriod']['Start'],  #write into csv file
								'LinkedAccount': group['Keys'][1],
								'Service': group['Keys'][0],
								'Amount': amount,
								'Unit': unit,
								'Estimated': results_by_time['Estimated']})


				print(results_by_time['TimePeriod']['Start'], '\t', '\t'.join(group['Keys']), '\t', amount, '\t', unit, '\t', results_by_time['Estimated'])


