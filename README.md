# Exporting-traces-from-OCEL
Many traditional tools do not support OCEL, therefore, the data needs to be split into traces. 

This project converts **OCEL logs** (`.jsonocel`) into **XES traces** using four methods:

1. **Per object type** (extended table)  
2. **Per object type** (objects/relations)  
3. **Connected components** (event–object bipartite graph)  
4. **Per day** (group by event date)

---

## Install

```bash
pip install -r requirements.txt
