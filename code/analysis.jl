using CSV,DataFrames,Statistics,Plots,Dates

path = pwd()
wam = CSV.read(path * "/data/ny_fed_wam_data_2003_2025.csv", DataFrame)
remit = CSV.read(path * "/data/remittances_to_treasury.csv", DataFrame)


wam[!, :δ] = 1 ./ wam.wam
f_data = innerjoin(wam, remit, on = :asOfDate)
f_data = rename(f_data, Dict("RESPPLLOPNWW" => "remittances_to_treasury"))

CSV.write(path * "/data/fed_remittances_wam.csv", f_data)

qduration = quantile(f_data.δ, [0.25, 0.5, 0.75])

f_data[!, :delta_category] = ifelse.(f_data.δ .<= qduration[1], "low",
                              ifelse.(f_data.δ .<= qduration[2], "medium-low",
                              ifelse.(f_data.δ .<= qduration[3], "medium-high",
                              "high")))

CSV.write(path * "/data/fed_remittances_wam_with_states.csv", f_data)