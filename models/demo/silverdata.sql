{{ config( materialized = 'table',schema ='Sliver' ) }}


with pos
as
(  
   --Handling duplicate values  using QUALIFY clause and ROW_NUMBER() function
     select * from layer_bronze.UPLOADFILES_CLIENT_DATA_CSV
      qualify row_number() over(partition by recordnumber order by recordnumber)=1 order by RECORDNUMBER

)
,transformed as(

    --converting data type from string to date on tranformed data
    select  RecordNumber , ClientID,ProductType, AgreementID, Value,
     currency, to_date(AsOFDate,'dd-MM-yyyy') as AsOFDate, Collateralized, 
     Counterparty, CounterpartyID, TypeofQFC,_airbyte_normalized_at as CreatedTime from pos
)
,final as(

    --Adding Audit Columns(TransID,RecID,Createdby and Created time,Updated by and Updated time)
    select *, '{{invocation_id}}' as TransID, 
    row_number() over(partition by TransID order by RecordNumber) as RecID,
    current_user() as CreatedBy,current_user() as UpdatedBy,
    to_timestamp_tz(current_timestamp()) as UpdatedTime from transformed
)
select * from final 
where  recordnumber is not null  
and clientid is not null
and ( producttype != '' and producttype is not null)
and (producttype not RLIKE '[^a-zA-Z0-9]' and typeofqfc not rlike '[^a-zA-Z0-9]')
and ASOFDATE like '____-__-__%'
and Collateralized in ('Y','N')




