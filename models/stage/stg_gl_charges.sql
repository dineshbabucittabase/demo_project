{{
    config(
        materialized='incremental',
        unique_key=['lng_GL_Charges_Id_Nmbr','EFFECTIVE_START_DATE'],
        incremental_strategy = 'merge',
        incremental_predicates = [
            "DBT_INTERNAL_DEST.EFFECTIVE_END_DATE > CURRENT_TIMESTAMP()"
        ],
        on_schema_change='sync_all_columns'
    )
}}

with incremental_fivetran_gl_charges_data as 
(
    SELECT
        NULL::NUMBER(38,0) AS ID_KEY,
        lng_GL_Charges_Id_Nmbr,
        lng_Res_Legs_Id_Nmbr,
        dtm_GL_Charges_Date,
        str_GL_Charges_Desc,
        mny_GL_Charges_Amount,
        mny_GL_Charges_Discount,
        mny_GL_Charges_Taxes,
        mny_GL_Charges_Total,
        str_GL_Charges_Notes,
        lng_GL_Charge_Type_Id_Nmbr,
        lng_Tax_Configuration_Id_Nmbr,
        lng_GL_Discounts_Id_Nmbr,
        str_Refundable_Charge,
        str_Visible_Flag,
        lng_Reservation_Nmbr,
        lng_GL_SurCharge_Id_Nmbr,
        mny_GL_Currency_Charges_Amount,
        mny_Exchange_Rate,
        lng_Currency_Id_Nmbr,
        mny_GL_Currency_Charges_Discount,
        mny_GL_Currency_Charges_Taxes,
        mny_GL_Currency_Charges_Total,
        tsp_Timestamp,
        dtm_Creation_Date,
        lng_Creation_User_Id_Nmbr,
        dtm_Last_Mod_date,
        lng_Last_Mod_User_Id_Nmbr,
        str_Private_Flag,
        bit_Fully_Paid,
        str_Doc_Type_Flag,
        str_Code,
        str_Sub_Code,
        _FIVETRAN_DELETED,
        _FIVETRAN_SYNCED,
        {{ stage_metadata_columns() }}
    FROM
        {{ source("FIVETRAN_SOURCE", "RAW_GL_CHARGES") }}

),

{% if is_incremental() %}

end_date_old_records as 
(
    SELECT
        STG_GL_CHARGES.lng_GL_Charges_Id_Nmbr AS ID_KEY, /*ID---*/
        STG_GL_CHARGES.lng_GL_Charges_Id_Nmbr,
        STG_GL_CHARGES.lng_Res_Legs_Id_Nmbr,
        STG_GL_CHARGES.dtm_GL_Charges_Date,
        STG_GL_CHARGES.str_GL_Charges_Desc,
        STG_GL_CHARGES.mny_GL_Charges_Amount,
        STG_GL_CHARGES.mny_GL_Charges_Discount,
        STG_GL_CHARGES.mny_GL_Charges_Taxes,
        STG_GL_CHARGES.mny_GL_Charges_Total,
        STG_GL_CHARGES.str_GL_Charges_Notes,
        STG_GL_CHARGES.lng_GL_Charge_Type_Id_Nmbr,
        STG_GL_CHARGES.lng_Tax_Configuration_Id_Nmbr,
        STG_GL_CHARGES.lng_GL_Discounts_Id_Nmbr,
        STG_GL_CHARGES.str_Refundable_Charge,
        STG_GL_CHARGES.str_Visible_Flag,
        STG_GL_CHARGES.lng_Reservation_Nmbr,
        STG_GL_CHARGES.lng_GL_SurCharge_Id_Nmbr,
        STG_GL_CHARGES.mny_GL_Currency_Charges_Amount,
        STG_GL_CHARGES.mny_Exchange_Rate,
        STG_GL_CHARGES.lng_Currency_Id_Nmbr,
        STG_GL_CHARGES.mny_GL_Currency_Charges_Discount,
        STG_GL_CHARGES.mny_GL_Currency_Charges_Taxes,
        STG_GL_CHARGES.mny_GL_Currency_Charges_Total,
        STG_GL_CHARGES.tsp_Timestamp,
        STG_GL_CHARGES.dtm_Creation_Date,
        STG_GL_CHARGES.lng_Creation_User_Id_Nmbr,
        STG_GL_CHARGES.dtm_Last_Mod_date,
        STG_GL_CHARGES.lng_Last_Mod_User_Id_Nmbr,
        STG_GL_CHARGES.str_Private_Flag,
        STG_GL_CHARGES.bit_Fully_Paid,
        STG_GL_CHARGES.str_Doc_Type_Flag,
        STG_GL_CHARGES.str_Code,
        STG_GL_CHARGES.str_Sub_Code,
        STG_GL_CHARGES._FIVETRAN_DELETED,
        STG_GL_CHARGES._FIVETRAN_SYNCED,
        STG_GL_CHARGES.TGT_CREATE_DT,
        CURRENT_TIMESTAMP() AS TGT_UPDATE_DT,        
        STG_GL_CHARGES.EFFECTIVE_START_DATE,
        CURRENT_TIMESTAMP() AS EFFECTIVE_END_DATE,
        'N' AS ACTIVE_FLAG       
    FROM
        {{ this }} STG_GL_CHARGES
    INNER JOIN 
        incremental_fivetran_gl_charges_data RAW_TBL_GL_CHARGES
    ON
        STG_GL_CHARGES.lng_GL_Charges_Id_Nmbr = RAW_TBL_GL_CHARGES.lng_GL_Charges_Id_Nmbr /*ID---*/
    AND 
        STG_GL_CHARGES.EFFECTIVE_END_DATE > CURRENT_TIMESTAMP()

),

{% endif %}

gl_charges_data_union as (
    select * from incremental_fivetran_gl_charges_data
    {% if is_incremental() %}
    union all
    select * from end_date_old_records
    {% endif %}
)

select * from gl_charges_data_union