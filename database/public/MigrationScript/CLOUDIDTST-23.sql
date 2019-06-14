ALTER TABLE public.td_stp_entry
    ALTER COLUMN pin TYPE text;

COMMENT ON TABLE public.td_stp_entry
    IS 'Version: 201802061300
Change log
2018-02-06: Fixed CLOUDIDTST-23
';




ALTER TABLE public.td_stp_exit
    ALTER COLUMN pin TYPE text;

COMMENT ON TABLE public.td_stp_exit
    IS 'Version: 201802061300
Change log
2018-02-06: Fixed CLOUDIDTST-23
';



ALTER TABLE public.td_stp_issue
    ALTER COLUMN pin TYPE text;

COMMENT ON TABLE public.td_stp_issue
    IS 'Version: 201802061300
Change log
2018-02-06: Fixed CLOUDIDTST-23
';



