[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared Past5YearsSalesData_All = let
  Source = Excel.Workbook(Web.Contents("https://globalgroupsl-my.sharepoint.com/personal/fabricadmin1_gritires_com/Documents/Apps/Microsoft Power Query/Uploaded Files/Past5YearsSalesData_All.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "Past5YearsSalesData_All", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"InvoiceDate", type date}, {"Invoice_no", Int64.Type}, {"Customer_ID", type text}, {"Customer_Name", type text}, {"BU", type text}, {"ItemBrand", type text}, {"Weight", type number}, {"Invoice Value", type number}})
in
  #"Changed column type";
