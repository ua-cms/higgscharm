import awkward as ak

events = ak.from_parquet("/eos/user/p/pkatris/higgscharm/outputs/hplusc/2022postEE/SomeSMSignal/base/SomeSMSignal_2_chunk44939.parquet")

print(events.fields)