import great_expectations as gx
import os
from config.settings import DATA_DIR, DATA_FILENAME, FORECAST_NDAYS, DATADOC_SITE_CONFIG, DOTENV_PATH
from dotenv import load_dotenv

def validate_data():

    # ref: https://docs.greatexpectations.io/docs/core/connect_to_data/filesystem_data
    context = gx.get_context()

    # setup data docs
    datadoc_sitename = 'datadoc_site'
    context.add_data_docs_site(
        site_name = datadoc_sitename
        , site_config = DATADOC_SITE_CONFIG
    )

    # setup data source
    datasource_name = 'datasource'
    data_source = context.data_sources.add_pandas_filesystem(
        name=datasource_name
        , base_directory=DATA_DIR
    )

    # setup data asset
    asset_name = 'dataasset'
    data_asset = data_source.add_csv_asset(
        name=asset_name
    )

    # Build batch request
    batch_definition_name = DATA_FILENAME
    # path below will be appended to the base_directory above
    batch_definition_path = DATA_FILENAME
    batch_definition = data_asset.add_batch_definition_path(
        name=batch_definition_name
        , path=batch_definition_path
    )
   
    # create expectation suite
    expectation_suite_name = 'expectationsuite'
    suite = context.suites.add(
        gx.ExpectationSuite(name=expectation_suite_name)
    ) 
    
    # define expectations
    ## count expectation (whole file in a single batch)
    e_count = gx.expectations.ExpectTableRowCountToEqual(
        value=FORECAST_NDAYS*24 # hourly data
    )

    ## not null expectations
    e_date_notnull = gx.expectations.ExpectColumnValuesToNotBeNull(column='date')
    e_temp_notnull = gx.expectations.ExpectColumnValuesToNotBeNull(column='temperature_2m')
    e_precip_notnull = gx.expectations.ExpectColumnValuesToNotBeNull(column='precipitation')
    e_humid_notnull = gx.expectations.ExpectColumnValuesToNotBeNull(column='relative_humidity_2m')
    e_apptemp_notnull = gx.expectations.ExpectColumnValuesToNotBeNull(column='apparent_temperature')
    e_vis_notnull = gx.expectations.ExpectColumnValuesToNotBeNull(column='visibility')
    e_cloud_notnull = gx.expectations.ExpectColumnValuesToNotBeNull(column='cloud_cover')
    
    ## Type expectations
    e_temp_type = gx.expectations.ExpectColumnValuesToBeOfType(column='temperature_2m', type_='float')
    e_precip_type = gx.expectations.ExpectColumnValuesToBeOfType(column='precipitation', type_='float')
    e_humid_type = gx.expectations.ExpectColumnValuesToBeOfType(column='relative_humidity_2m', type_='float')
    e_apptemp_type = gx.expectations.ExpectColumnValuesToBeOfType(column='apparent_temperature', type_='float')
    e_vis_type = gx.expectations.ExpectColumnValuesToBeOfType(column='visibility', type_='float')
    e_cloud_type = gx.expectations.ExpectColumnValuesToBeOfType(column='cloud_cover', type_='float')
    
    # Populate suite with expectations
    suite.add_expectation(e_count)
    suite.add_expectation(e_date_notnull)
    suite.add_expectation(e_temp_notnull)
    suite.add_expectation(e_precip_notnull)
    suite.add_expectation(e_humid_notnull)
    suite.add_expectation(e_apptemp_notnull)
    suite.add_expectation(e_vis_notnull)
    suite.add_expectation(e_cloud_notnull)
    suite.add_expectation(e_temp_type)
    suite.add_expectation(e_precip_type)
    suite.add_expectation(e_humid_type)
    suite.add_expectation(e_apptemp_type)
    suite.add_expectation(e_vis_type)
    suite.add_expectation(e_cloud_type)

    # Define validator
    validation_def_name = 'validation_definition'
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            data=batch_definition
            , suite=suite
            , name=validation_def_name
    ))

    # setup checkpoint
    load_dotenv(dotenv_path=DOTENV_PATH)
    action_list = [
        gx.checkpoint.actions.EmailAction(
            name = 'send_email_after_validating'
            , smtp_address = 'smtp.gmail.com'
            , smtp_port = '587'
            , receiver_emails = os.getenv('RECEIVER_EMAIL')
            , notify_on = 'all'
            , use_tls=True
            , sender_login=os.getenv('SENDER_EMAIL')
            , sender_password=os.getenv('SENDER_EMAIL_APP_PASSWORD')
        )
        , gx.checkpoint.actions.UpdateDataDocsAction(
            name = 'update_datadoc_site'
            , site_names = [datadoc_sitename]
        )
    ]
    checkpoint_name = 'checkpoint'
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name=checkpoint_name
            , validation_definitions=[validation_definition]
            , actions=action_list
        )
    )

    # Run validation
    validation_results = checkpoint.run()
    print(f'Validation is successful: {validation_results.success}')
    return validation_results.success
    

if __name__=="__main__":
    validate_data()