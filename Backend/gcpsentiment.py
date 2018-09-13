from google.cloud import language
from google.cloud.language import enums
from google.oauth2 import service_account

def analyzeSentiments(headlines):
	credentials = service_account.Credentials.from_service_account_file('calhacks_key.json')
	scoped_credentials = credentials.with_scopes(['https://www.googleapis.com/auth/cloud-platform'])
	client = language.LanguageServiceClient(credentials=scoped_credentials)
	weightedsum = 0.0
	weighttotal = 0.0
	for headline in headlines:
		document = language.types.Document(
			content=headline,
			type=enums.Document.Type.PLAIN_TEXT
			)
		sentiment = client.analyze_sentiment(document=document).document_sentiment
		weightedsum += (sentiment.magnitude + 1.0) * sentiment.score
		weighttotal += sentiment.magnitude + 1.0
	return weightedsum / weighttotal
