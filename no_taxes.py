'''
Created on Feb 24, 2015

@author: nfergusn
'''

import numpy as np
import pandas as pd
from pandas import DataFrame, Series
import collections
import csv
import os
from fuzzywuzzy import fuzz
import string
import re
import difflib as dl
from numpy import average
from docutils.nodes import Inline
import statsmodels.api

os.chdir(r'/media/sf_Dropbox/cpgis/MIDT//asmt')

def no_tax_list():
    lst_tax = []
    with open('missing_city_taxes.csv','rb') as f:
        r = csv.reader(f)
        r.next()
        for row in r:     
            lst_tax.append(row[2].strip())
    return lst_tax

#taxes = set(lst_tax)    
 
def assessor_table(table):
    '''
    builds nested dictionary containing records from assessor's table
    whose parcelid is contained in missing tax table
    nested key values are derived from table header     
    '''
    assessor_dict = collections.defaultdict(list)            
    with open(table, 'rb') as f:
        reader = csv.reader(f)
        header = [i.lower() for i in reader.next() if i != 'PARID']
        for row in reader:
            if row[0] in lst_tax:
                assessor_dict[row[0]] = {header[i]: row[ i + 1] for i in range(len(header))}
    return assessor_dict    

def luc_codes():
    luc_desc = collections.defaultdict(str)
    with open('AEDIT.txt', 'rb') as f:
        reader = csv.reader(f)
        reader.next()
        for row in reader:
            if row[0] == 'ASMT' and row[1] == 'LUC':
                print row[2], row[3].split('-')[1].strip()
                luc_desc[row[2]] = row[3].split('-')[1].strip()
    return luc_desc
        
def owners_match(own1, own2):
    '''
    >>>match_owners('dockery charles', 'docker charlie')
    False
    >>>match_owners('dawning investments llc series a', 'dawning investments llc series f')
    True
    >>>match_owners('davis lillian', 'davis lillian c')
    True
    >>>match_owners('d and d enterprise', 'd and d enterprise llc')
    True
    >>>match_owners('burress leland & shirley','burress leland jr & shirley j)
    True
    >>>match_owners('smith jasper and kenney r smith and', 'smith jasper and kenny r smith and')
    True
    >>>match_owners('smith mary e', 'smith mary g')
    False    
    '''
    if own1 == own2:
        pass
    else:
        return fuzz.partial_ratio(own1, s2)
    
def clean_string(st = ''):
    '''
    takes raw string from assessor's database and returns a standardized version
    in lower case without any punctuation
    '''
    st_split = st.split(' ')
    punctuation = string.punctuation
    st = ' '.join(str(a).lower().strip() for a in st_split if a != '')
    return re.sub('\s\s+',' ',re.sub('[{0}]'.format(punctuation),'', st))

def remove_values(s1,ls):
    """ s1 --> string to match
        list1 --> list of closest matches returned from difflib.get_close_matches
        return --> a list containing the closest matching string, and a sequence 
                    of match ratios generated from fuzzywuzzy module
    
    difflib.get_close_matches returns matches in decreasing order of match
    with the first result always being an exact match because it's included
    in the full list, therefore, picking the second in the list means that 
    it's the next closest match besides its match"""
    drop_vals = filter(lambda x:match_score(s1,x) > 75 and match_score(s1,x) <100, ls)
    return drop_vals

def match_score(s1, s2):
    ratio = fuzz.ratio(s1,s2)
    token_ratio = fuzz.token_set_ratio(s1, s2)
    partial_ratio = fuzz.token_sort_ratio(s1,s2)
    avg = np.mean((ratio,token_ratio,partial_ratio))
    return avg        
    
def next_largest(list1):
    m = max(list1)
    return max(n for n in list1 if n != m) 
      
def accuracy(min_distance):
    est_own = df_dup[df_dup.own_distance >= min_distance]
    est_addr =  df_dup[(df_dup.adr_distance >= min_distance) & (df_dup.adr_distance < 100)]        
    own_count = float(len(est_own.groupby('OWN1_x').count()))
    addr_count = float(len(est_addr.groupby('OWN1_x').count()))
    return (own_count,addr_count)


#Compare Ownership

def tax_list_count(dict, field):
    count = collections.defaultdict(int)
    for par in lst_tax:
        if par in dict.keys():
            luc = luc_desc[dict[par][field]]
            if count[luc]:
                count[luc] += 1
            else:
                count[luc] = 1  
    
def calculate_tax(dict):
    '''
    TODO: calculate estimated total lost tax
    '''
    pass

def concat(*args):
    strs = [str(arg) for arg in args if not pd.isnull(arg)]
    return ' '.join(strs) 
np_concat = np.vectorize(concat)

owndat = pd.read_csv('OWNDAT.txt', keep_default_na=False)
owndat = pd.read_csv('OWNDAT.txt',keep_default_na=False,
                     dtype={k:'str' for k in owndat.columns})

#string concatenation of addresss components
owndat['adrcomp'] = np_concat(owndat.ADRNO, owndat.ADRDIR, owndat.ADRSTR, 
                              owndat.ADRSUF, owndat.CITYNAME, owndat.STATECODE, 
                              owndat.ZIP1)

no_tax_list = pd.read_excel('City Only Properties Available for TXS 2012 PY.xlsx')
no_tax_list['CityParcelNo'] = no_tax_list.apply(lambda x: x['CityParcelNo'].rstrip(), axis=1)
owndat = owndat[owndat.PARID.isin(no_tax_list.CityParcelNo)]#eliminate records not in no tax list


#clean and standardize all strings to be used for comparison
owndat.adrcomp = owndat.adrcomp.apply(clean_string)
owndat.OWN1 = owndat.OWN1.apply(clean_string)
owndat.OWN2 = owndat.OWN2.apply(clean_string)
owndat.adrcomp = owndat.adrcomp.apply(clean_string)

############################################### section 2 #####################################
owndat_sample = owndat
owndat_sample = owndat_sample.drop(['OWN2', 'ADRNO', 'ADRDIR', 'ADRSTR', 
                                    'ADRSUF', 'ADRSUF2', 'CITYNAME', 'STATECODE', 
                                    'UNITNO', 'UNITDESC', 'ADDR1', 'ADDR2', 
                                    'ADDR3', 'ZIP1', 'ZIP2', 'NOTE1', 'NOTE2'],axis=1)
owndat_sample['dummy'] = 0
df_dup = pd.merge(owndat_sample, owndat_sample, on='dummy')
del df_dup['dummy']

#remove records joined with themselves, results in 8281 records dropped
df_dup = df_dup[df_dup.OWN1_x != df_dup.OWN1_y]
"""create common key and drop instances where (B,A) == (A,B)
    commonality established by sorting and joining parcelid pairs
"""
df_dup['common_key'] = df_dup.apply(lambda x: ''.join(a for a in sorted([x['OWN1_x'],x['OWN1_y']])),axis=1)
df_dup = df_dup.drop_duplicates(['common_key'])
#match scores for owners and addresses
df_dup['own_distance'] = df_dup.apply(lambda r: match_score(r['OWN1_x'], r['OWN1_y']), axis=1)
df_dup['adr_distance'] = df_dup.apply(lambda r: match_score(r['adrcomp_x'], r['adrcomp_y']), axis=1)

q = '(adr_distance >= 85 & (adr_distance >= 95 & adr_distance < 100)) | adr_distance == 100'
df_select = df_dup.query(q)

df_select.to_csv('draft_duplicates.csv')


################################################ end section 2################################

own_corrected = pd.read_csv('draft_duplicates_corrected.csv')
#join corrected ownership on "misspelled" name to update original ownership name
owndat_merge = pd.merge(owndat,own_corrected, left_on='OWN1',right_on='OWN1_y', how='outer')
owndat_merge.to_csv('owndat_merge.csv',header=True,na_rep="NA",columns=('PARID', 'OWN1', 
                                                                        'adrcomp','PARID_x', 
                                                                        'PARID_y', 'OWN1_x', 
                                                                        'OWN1_y', 'adrcomp_x', 
                                                                        'adrcomp_y', 'own_distance',
                                                                         'adr_distance', 'correct'))
#update original OWN1 field with predicted value
owndat_merge.loc[owndat_merge['correct']==1,'OWN1'] = owndat_merge.OWN1_x
owndat_merge = owndat_merge.drop(['PARID_x', 'PARID_y', 'OWN1_x', 'OWN1_y', 'adrcomp_x', 'adrcomp_y', 'own_distance', 'adr_distance', 'correct'], axis=1)

#prepare luc values and descriptions from pardat and aedit table
aedit = pd.read_csv('aedit.txt')
pardat = pd.read_csv('pardat.txt')
pardat = pd.read_csv('pardat.txt',dtype={k:'str' for k in pardat.columns})
pardat['LUC'] = pardat.LUC.apply(lambda x: str(x).zfill(3))
pardat_sub = pardat[['PARID','LUC', 'ADRNO', 'ADRADD', 'ADRDIR', 'ADRSTR', 'ADRSUF', 'ZIP1']]
zips = pd.read_csv('zipcodes.csv')
zips = pd.read_csv('zipcodes.csv', dtype={k: 'str' for k in zips.columns})


pardat_sub = pd.merge(pardat_sub, zips, left_on='ZIP1', right_on='ZIP')

pardat_sub['adrpar'] = np_concat(pardat_sub.ADRNO, pardat_sub.ADRADD, pardat_sub.ADRDIR,pardat_sub.ADRSTR,pardat_sub.ADRSUF,pardat_sub.PO_NAME,pardat_sub.STATE,pardat_sub.ZIP1)
pardat_sub.adrpar = pardat_sub.adrpar.apply(clean_string)

pardat_m = pardat_sub[['PARID','adrpar']]
owndat_m = owndat_merge[['PARID', 'adrcomp']]
par_own_merge = pd.merge(pardat_m, owndat_m, on='PARID')
par_own_merge['adr_dist'] = par_own_merge.apply(lambda r: match_score(r['adrpar'], r['adrcomp']), axis=1)

luc = pd.merge(pardat_sub,aedit[(aedit['FLD']=='LUC') & (aedit['TBLE'] == 'PARDAT')], left_on="LUC", right_on="VAL",how='outer')
#remove '-' from MSG field
rep = lambda x: x.replace('- ', '')
luc['MSG'] = luc.MSG.apply(rep)

llc_props = own_corrected[(own_corrected['own_distance'] < 50) & (own_corrected['adr_distance'] >= 99) & (own_corrected['correct'] == 1)]

#combine everything together and summarize
owndat_merge = owndat_merge.drop_duplicates(['PARID'])
owndat_merge = pd.merge(owndat_merge,luc,on='PARID')
counts_owners = owndat_merge.groupby('OWN1').size()

counts_owners_df = counts_owners[counts_owners >= 5].to_frame('COUNT')
counts_owners_df['OWN1'] = counts_owners_df.index
owndat_merge_count = pd.merge(owndat_merge, counts_owners_df, on = 'OWN1')
owndat_merge_count = pd.merge(owndat_merge_count, no_tax_list, left_on = 'PARID', right_on = 'CityParcelNo')
owndat_merge_count.to_csv('owner_to_parcel_more_5.csv', columns = ['PARID', 'adrpar','OWN1', 
                                                                   'adrcomp','MSG', 'COUNT',
                                                                   'MinOfTaxYear', 'MaxOfTaxYear',
                                                                   'SumOfYearTotalAmountDue','SumOfTotalParcelBalance'])

counts_luc = owndat_merge.groupby('MSG').size()
amounts_total = pd.merge(owndat_merge,no_tax_list, left_on='PARID', right_on='CityParcelNo')
amounts_total = amounts_total[['OWN1', 'SumOfYearTotalAmountDue','SumOfTotalParcelBalance']]
amounts_total_grouped = amounts_total.groupby(amounts_total['OWN1'])


owndat_merge.to_csv('nt_final.csv', header=True,sheet_name='All_Results')
counts_owners.to_csv('nt_final_owner_count.csv', header=True)
counts_luc.to_csv('nt_final_luc_count.csv',header=True)
par_own_merge.to_csv('nt_final_home_owner.csv', header=True)
llc_props.to_csv('llcs.csv', header=True)
amounts_total_grouped.agg(['count','sum']).to_csv('Ownership_Balance_Totals.csv', header=True)


################################################ output ########################################

#evaluate threshold for most accurate name match
thresholds = range(1,101)
o =  []
a = []

est = DataFrame(columns=('threshold','own_count','addr_count'))
for t in thresholds:
    acc = accuracy(t)
    est.set_value(t,'threshold',t) 
    est.set_value(t,'own_count',acc[0])
    est.set_value(t, 'addr_count',acc[1])

x  = sm.add_constant(est[['own_count', 'addr_count']])
reg = sm.OLS(est['threshold'],x).fit()
'>>>%matplotlib inline'
import pylab

pylab.scatter(est.own_count, est.threshold, est.addr_count)
pylab.plot(thresholds, r)
pylab.plot(r,p)
pylab.legend(['precision','recall'],loc=2)
################################################ end output ###################################

if __name__ == '__main__':
    pass