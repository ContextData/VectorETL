from .base import BaseSource
from .file_loader import FileBaseSource
from .s3_loader import S3Source
from .box_loader import BoxSource
from .database_loader import DatabaseSource
from .dropbox_loader import DropboxSource
from .stripe_loader import StripeSource
from .zendesk_loader import ZendeskSource
from .google_drive import GoogleDriveSource
from .google_cloud_storage import GoogleCloudStorageSource
from .local_file import LocalFileSource
from .airtable_loader import AirTableSource
from .google_bigquery import GoogleBigQuerySource
from .hubspot_loader import HubSpotSource
from .zoho_crm_loader import ZohoCrmSource
from .zoho_desk_loader import ZohoDeskSource
from .intercom_loader import InterComSource
from .paystack_loader import PayStackSource
from .flutterwave_loader import FlutterWaveSource
from .gmail_loader import GmailSource
from .mailchimp_loader import MailChimpMarketingSource

def get_source_class(config):
    source_type = config['source_data_type']
    if source_type == 'Amazon S3':
        return S3Source(config)
    if source_type == 'Box':
        return BoxSource(config)
    elif source_type == 'Local File':
        return LocalFileSource(config)
    elif source_type == 'database':
        return DatabaseSource(config)
    elif source_type == 'Dropbox':
        return DropboxSource(config)
    elif source_type == 'stripe':
        return StripeSource(config)
    elif source_type == 'zendesk':
        return ZendeskSource(config)
    elif source_type == 'Google Drive':
        return GoogleDriveSource(config)
    elif source_type == 'Google Cloud Storage':
        return GoogleCloudStorageSource(config)
    elif source_type == 'AirTable':
        return AirTableSource(config)
    elif source_type == 'Google BigQuery':
        return GoogleBigQuerySource(config)
    elif source_type == 'HubSpot':
        return HubSpotSource(config)
    elif source_type == 'ZohoCrm':
        return ZohoCrmSource(config)
    elif source_type == 'ZohoDesk':
        return ZohoDeskSource(config)
    elif source_type == "InterCom":
        return InterComSource(config)
    elif  source_type == 'PayStackS':
        return PayStackSource(config)
    elif source_type == "FlutterWave":
        return FlutterWaveSource(config)
    elif source_type == "Gmail":
        return GmailSource(config)
    elif source_type == "MailChimp":
        return MailChimpMarketingSource(config)
    else:
        raise ValueError(f"Unsupported source type: {source_type}")
