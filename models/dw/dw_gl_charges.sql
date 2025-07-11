{{
    config(
        materialized='incremental',
        unique_key=['lng_GL_Charges_Id_Nmbr'],
        incremental_strategy = 'merge'
    )
}}

with vw_gl_charges as (

    select 
        t1.lng_GL_Charges_Id_Nmbr,
        t1.lng_Reservation_Nmbr,
        t1.dtm_GL_Charges_Date,
        TO_DATE((CONVERT_TIMEZONE('UTC','America/Denver',t1.dtm_GL_Charges_Date))) as  Charge_Date_MST,
        t1.lng_Res_Legs_Id_Nmbr,
        t1.lng_GL_Charge_Type_Id_Nmbr,
        t1.lng_GL_Charge_Type_Id_Nmbr AS Charge_Type,
        t1.mny_GL_Charges_Amount - t1.mny_GL_Charges_Discount as Net_Charge,
        t1.mny_GL_Charges_Amount,
        t1.mny_GL_Charges_Discount, --Commercial_Views
        t1.mny_GL_Charges_Taxes,
        t1.mny_GL_Charges_Total,
        t1.STR_GL_CHARGES_DESC,
        t1.mny_GL_Currency_Charges_Amount,
        t1.mny_GL_Currency_Charges_Discount,
        t1.mny_GL_Currency_Charges_Taxes,
        t1.lng_Creation_User_Id_Nmbr,
        t1.mny_Exchange_Rate,
        t1.mny_GL_Currency_Charges_Total,
        t1.DTM_CREATION_DATE,
        t4.str_Leg_Status,
        t4.lng_Leg_Nmbr,
        t5.str_Ident as Departure,
        t6.str_Ident as Arrival,
        CURRENT_TIMESTAMP() TGT_UPDATE_DT
   FROM  
           {{ ref('vw_gl_charges') }}    AS t1 
        LEFT JOIN
            {{ ref('vw_res_legs') }}  AS t4 
        ON
            t1.lng_Res_Legs_Id_Nmbr = t4.lng_Res_Legs_Id_Nmbr
        LEFT JOIN
            {{ ref('vw_airport') }} AS t5 
        ON 
            t4.lng_Dep_Airport_Id_Nmbr = t5.lng_Airport_Id_Nmbr
        LEFT JOIN
            {{ ref('vw_airport') }}  AS t6 
        ON
            t4.lng_Arr_Airport_Id_Nmbr = t6.lng_Airport_Id_Nmbr
{% if is_incremental() %}            
        where     
        (t1.TGT_UPDATE_DT   >= CURRENT_DATE()
        OR 
        t4.TGT_UPDATE_DT   >= CURRENT_DATE()) 
{% endif %}
        
)
select * from  vw_gl_charges