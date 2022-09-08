from itertools import islice

from hapi2.config import SETTINGS, VARSPACE

from .base import sql, query, commit, bindparam

def get_first_available_(cls,col,local=True):
    """
    Fetch next from maximum occupied value of the column from database for given class.
    """
    session = VARSPACE['session']
    
    if local:
        stmt = sql.select(
            [sql.func.min(getattr(cls.__table__.c,col))]
        )
        d_ = -1
    else:
        stmt = sql.select(
            [sql.func.max(getattr(cls.__table__.c,col))]
        )
        d_ = 1
    res = session.execute(stmt)

    curval = res.first()[0]
    if curval is None: 
        first_avail = 0
    else:
        first_avail = curval + d_
    
    return first_avail,d_
            
CHUNK_SIZE = 998 # maximun number of parameters in SQLite query minus one (normally 998)   
def chunks(lst,n=CHUNK_SIZE): # needed to avoid the SQLite error ("too many SQL variables" if N_IDS>999)
    for i in range(0,len(lst),n):
        yield lst[i:i+n]            

TRANS_ATTRS = ['molec_id','local_iso_id','nu','sw','a','gamma_air','gamma_self','elower',
    'n_air','delta_air','global_upper_quanta','global_lower_quanta','local_upper_quanta',
    'local_lower_quanta','ierr','iref','line_mixing_flag','gp','gpp']
            
def insert_transition_dicts_core_(cls,TRANS_DICTS,linelist_id,local,initial=False):
    """
    Helper function inserting the block of transitions in dictionary format in the database. 
    INPUT: 
        TRANS_DICTS: list of dicts, each dict holding all needed parameters for one transition
        linelist_id: id of the linelist to attach the transitions to
        local: flag to separate "global" (i.e. those from HITRANonline) transitions and "local" ones
        initial: True means that some lookups are omitted, which speeds up the initial line addition
    """
    session = VARSPACE['session']    
    
    #Transition = VARSPACE['db_backend'].models.Transition
    Transition = cls
    IsotopologueAlias = VARSPACE['db_backend'].models.IsotopologueAlias
    linelist_vs_transition = VARSPACE['db_backend'].models.linelist_vs_transition
        
    # split all dicts into two groups:
    #   1) new lines (data is inserted)
    #       -> 1A) new lines without IDs (need to assign free IDs before inserting)
    #       -> 1B) new lines with IDs
    #   2) existing lines (always have IDs, data are updated using ID values)
    
    # (*) "Item" dictionaries should be created only for groups 1A and 1B
    # (*) Transaction should be attached only to LineList, not for each transition.
    # (*) Isotopologue aliases should already exist in database; they are attached only to 1A and 1B.
    
    TRANS_DICTS_1A = [] # proxy list for group 1A (no IDs provided)
    TRANS_DICTS_1B = [] # proxy list for group 1B (IDs provided but not found in database)
    TRANS_DICTS_1A_1B = [] # proxy list for groups 1A(no IDs provided) + 1B(IDs provided but not found in database) 
    TRANS_DICTS_2 = [] # proxy list for group 2 (found in DB by IDs)
    
    ISOTOPOLOGUE_ALIASES = {} # dict: keys - alias names, values = db ids
    
    TRANS_DICTS_LOOKUP = {} # proxy dict for transitions with IDs for the database lookup
    
    # ===================================================================================
    # 1. Get first available ID to start filling the group 1A.
    # ===================================================================================

    ids = []
    id,d_id = get_first_available_(Transition,'id',local)
    
    # ===================================================================================
    # 2. Loop through all transitions to find the group 1A and prepare the lookup table.
    # ===================================================================================

    for trans_dict in TRANS_DICTS:
        
        # setup isotopologue alias lookup map
        isoal_name = trans_dict['isotopologue_alias']
        ISOTOPOLOGUE_ALIASES[isoal_name] = {'alias':isoal_name,'id':None}        
        
        if 'id_' not in trans_dict:
            if not local: # data from HITRANonline without IDs - something is wrong here
                raise Exception('data fetched from HITRANonline must contain valid ids')
            else:                
                trans_dict['id_'] = id
                TRANS_DICTS_1A_1B.append(trans_dict) # adding group 1A so far
                id += d_id
        else:
            id_ = trans_dict['id_']
            TRANS_DICTS_LOOKUP[id_] = trans_dict
        
        ids.append(trans_dict['id_'])
            
    # ===================================================================================
    # 3. Search and optionally save isotopologue aliases.
    # ===================================================================================
    
    isoal_id,_ = get_first_available_(IsotopologueAlias,'id',local=False) # no local aliases

    isoal_names = list(ISOTOPOLOGUE_ALIASES.keys())
    
    # lookup in the database
    DB_LOOKUP = session.execute(sql.select([
        IsotopologueAlias.__table__.c.alias,
        IsotopologueAlias.__table__.c.id,
    ]).where(IsotopologueAlias.__table__.c.alias.in_ (isoal_names)))
    DB_LOOKUP = list(DB_LOOKUP) # !!!
    
    isoal_names_found = [e[0] for e in DB_LOOKUP]
    isoal_names_not_found = list(set(isoal_names)-set(isoal_names_found))
    
    # Attach found ids to the ISOTOPOLOGUE_ALIASES map
    for isoal_name,id in DB_LOOKUP:
        ISOTOPOLOGUE_ALIASES[isoal_name]['id'] = id 
            
    # Attach ids and virt_ids to the new isotopologue entries
    ISOAL_DICTS = []
    for isoal_name in isoal_names_not_found:
        #print('isoal_name',isoal_name,'from',isoal_names_not_found)
        
        # isoal dict
        isoal_dict = ISOTOPOLOGUE_ALIASES[isoal_name]
        isoal_dict['id'] = isoal_id
                
        isoal_id += 1
        
        ISOAL_DICTS.append(isoal_dict)
                
    # ===================================================================================
    # 4. Lookup the IDs to split the rest of transitions between groups 1B and 2
    # ===================================================================================
    
    ids_for_lookup = list(TRANS_DICTS_LOOKUP.keys()) # get the ids to lookup
    
    # The total lookup is split into several because the default engine (SQLite) doesn't support huge amounts of 
    # parameters passes in a single SQL query.
    # THIS CAN BE FURTHER OPTIMIZED BY APPLYING THE "COMPRESSION" TECHNIQUES ON ARRAYS OF NUMBERS,
    # SUCH AS CONVERTING ARRAY TO RANGE IN ORDER TO MINIMIZE THE NUMBER OF SUB-LOOKUPS. 
    # TO BE DONE.
    
    DB_LOOKUP = []
    total_read = 0
    
    if not initial:
        
        for i,ids_chunk in enumerate(chunks(ids_for_lookup)):
            n_chunk = len(ids_chunk)
            stmt = sql.select(
                    [Transition.__table__.c.id, 
                    Transition.__table__.c.extra]
                ).\
                where(
                    Transition.__table__.c.id.in_(ids_chunk)
                ) # retrieve only ids and extra parameters (because the latter should be updated with the new data)
            total_read += n_chunk
        
            DB_LOOKUP += session.execute(stmt) # list of tuples (id,extra)
                
    # Create group 2 and merge the new extra parameters there.
    for id,extra in DB_LOOKUP:
        trans_dict = TRANS_DICTS_LOOKUP[id]
        if extra is not None:
            trans_dict['extra'] = extra.update(trans_dict['extra'])
        TRANS_DICTS_2.append(trans_dict)
        TRANS_DICTS_LOOKUP.pop(id) # delete item from lookup, leaving only items from group 1B.
        
    # Now TRANS_DICTS_LOOKUP contains only items from group 1B, which should be added to group 1A in TRANS_DICTS_1A_1B
    TRANS_DICTS_1A_1B += [TRANS_DICTS_LOOKUP[id] for id in TRANS_DICTS_LOOKUP]
    
    # Groups 1A and 1B are similar in the way data are inserted:
    #   -> need to generate item objects + line list mappings.    
    # Group 2 doesn't need to generate only line list mappings (rest of the data already in the db)
    
    # Finally, attach isotopologue alias ids to TRANS_DICTS_1A_1B and TRANS_DICTS_2

    for trans_dict in TRANS_DICTS_1A_1B: # new items - delete isoal + link the id
        isoal_name = trans_dict['isotopologue_alias']
        trans_dict['isotopologue_alias_id'] = ISOTOPOLOGUE_ALIASES[isoal_name]['id']
        del trans_dict['isotopologue_alias']

    for trans_dict in TRANS_DICTS_2: # existing items - just delete isoal
        del trans_dict['isotopologue_alias']
    
    # ===================================================================================
    # 5. START ADDING LINES TO DATABASE
    # ===================================================================================
    
    # -- Step 5.1: create item objects for groups 1A and 1B --
    
    LLST_VS_TRANS = []
        
    for trans_dict in TRANS_DICTS_1A_1B:
        
        id_ = trans_dict['id_']

        # Rename id_ to id for the insert operation
        trans_dict['id'] = trans_dict.pop('id_')
                           
        # Start filling the line list mappings (list is common)
        llst_map_item = {'linelist_id':linelist_id,'transition_id':id_}
        LLST_VS_TRANS.append(llst_map_item)

    # -- Step 5.2: create line list mapping objects for group 2 --
    
    for trans_dict in TRANS_DICTS_2:
                
        id_ = trans_dict['id_']
        
        # Continue filling the line list mappings (list is common)
        llst_map_item = {'linelist_id':linelist_id,'transition_id':id_}
        LLST_VS_TRANS.append(llst_map_item)
                
    # -- Step 5.3: FINALLY ADD ITEMS !!!  --
    
    #print('==================================')
    #print('ISOAL_ITEM_DICTS (new)>>>')
    #print('==================================')
    #print(json.dumps(ISOAL_ITEM_DICTS,indent=3))
    #
    #print('==================================')
    #print('ISOAL_DICTS (new)>>>')
    #print('==================================')
    #print(json.dumps(ISOAL_DICTS,indent=3))
    #
    #print('==================================')
    #print('ISOTOPOLOGUE_ALIASES (new+exist)>>>')
    #print('==================================')
    #print(json.dumps(ISOTOPOLOGUE_ALIASES,indent=3))     
    #
    #print('==================================')
    #print('TRANS_ITEM_DICTS_1A_1B>>>')
    #print('==================================')
    #print(json.dumps(TRANS_ITEM_DICTS_1A_1B,indent=3))
    #
    #print('==================================')
    #print('TRANS_DICTS_1A_1B>>>')
    #print('==================================')
    #print(json.dumps(TRANS_DICTS_1A_1B,indent=3))
    #
    #print('==================================')
    #print('TRANS_DICTS_2>>>')
    #print('==================================')
    #print(json.dumps(TRANS_DICTS_2,indent=3))
    #
    #print('==================================')
    #print('LLST_VS_TRANS>>>')
    #print('==================================')
    #print(json.dumps(LLST_VS_TRANS,indent=3))
    
    #raise Exception # debug barrier
    
    # ---> add new isotopologue aliases
    if ISOAL_DICTS: 
        session.execute(IsotopologueAlias.__table__.insert(),ISOAL_DICTS) 
    
    # ---> add new transitions (groups 1A and 1B)
    if TRANS_DICTS_1A_1B:
        session.execute(Transition.__table__.insert(),TRANS_DICTS_1A_1B) 
    
    # ---> update existing transitions
    if TRANS_DICTS_2:
        # THE RIGHT WAY IS MULTIPLE WHERES, INSTEAD OF USING "AND" OPERATION!!!
        insert_values = {pname:bindparam(pname) for pname in TRANS_ATTRS+['extra',]}
        stmt = Transition.__table__.update().\
            where(
                Transition.__table__.c.id == bindparam('id_')
            ).\
            values(insert_values)
        session.execute(stmt,TRANS_DICTS_2)
        
    # ---> update line list mappings for existing transitions
    if LLST_VS_TRANS:
        session.execute(linelist_vs_transition.insert(),LLST_VS_TRANS) 
    
    session.commit() # COMMIT ALL CHANGES!!
    
    return ids

def __create_linelist_TMP__(llst_name):
    # !!! TEMPORARY FUNCTION FOR CREATING LINELIST 
    # (TO BE SUBSTITUTED BY THE GENERAIC CORE FUCTION)
    
    # Create a line list.
    session = VARSPACE['session']
    Linelist = VARSPACE['db_backend'].models.Linelist
    
    llst = session.query(Linelist).filter(Linelist.name==llst_name).first()
    if not llst:
        llst = Linelist(); llst.name = llst_name
        llst.id,_ = get_first_available_(Linelist,'id',local=False)
        session.add(llst)
    
    session.commit() 
    
    return llst   

def get_transitions_by_ids(ids):
    """
    Return generator returning transitions by ids.
    """
    return []

#NBULK = 300000 # EACH BULK CORRESPONDS TO SEPARATE TRANSACTION
NBULK = 150000 # EACH BULK CORRESPONDS TO SEPARATE TRANSACTION
#def __insert_transitions_core__(models,header,llst_name='DEFAULT'):   DELETE THIS LINE !!!!!!!
def __insert_transitions_core__(cls,stream,local=True,llst_name='default',**argv):
    """
    Update and commit exclusively for cross-section headers. Will not work for other types of objects!!!
    The name of the HAPI table should be supplied with the llst_name parameter.
    THIS VERSION USES SQLALCHEMY ORM FOR LINELIST AND CORE FOR TRANSITIONS
    """
    
    llst = __create_linelist_TMP__(llst_name)
        
    ids = []
    ntot = 0
    
    gen = stream.__iter__()
    print('==================================')
    while True:
        TRANS_DICTS = list(islice(gen,NBULK))
        if not TRANS_DICTS: break
        
        ntot += len(TRANS_DICTS)            
        ids += insert_transition_dicts_core_(cls,TRANS_DICTS,llst.id,local=local,**argv)

        print('Total lines processed: %d'%ntot)
        print('==================================')
                
    #return models.Linelist(llst_name).transitions # lazy; 
    # ATTENTION!!! This can return more lines than was downloaded
    # because LineList was decided not to be cleaned up!
    
    return get_transitions_by_ids(ids) # BETTER WAY OF RETURNING LINES!! (TODO)

def __insert_base_items_core__(cls,ITEM_DICTS,local=True):
    """
    Main procedure for inserting base item dicts using core.
       cls - item class

    The idea is to split the items into 2 groups:
       GROUP 1: New items (data are inserted)
           -> GROUP 1A: New items without IDs (need to assign free IDs before inserting)
           -> GROUP 1B: New items with IDs
       GROUP 2: Existing items (always have IDs, data are updated using ID values)
    """
    
    session = VARSPACE['session']

    ITEM_DICTS_1A = [] # proxy list for group 1A (no IDs provided)
    ITEM_DICTS_1B = [] # proxy list for group 1B (IDs provided but not found in database)
    ITEM_DICTS_1A_1B = [] # proxy list for groups 1A(no IDs provided) + 1B(IDs provided but not found in database) 
    ITEM_DICTS_2 = [] # proxy list for group 2 (found in DB by IDs)
    
    ITEM_DICTS_LOOKUP = {} # proxy dict for items with IDs for the database lookup    

    # ===================================================================================
    # 1. Get first available values for auto-filled parameters for the group 1A.
    # ===================================================================================
    
    id,d_id = get_first_available_(cls,'id',local)
    
    # ===================================================================================
    # 2. Loop through all items to find the group 1A and prepare the lookup table.
    # ===================================================================================

    ids = []

    for item_dict in ITEM_DICTS:
        if 'id' not in item_dict:
            ITEM_DICTS_1A_1B.append(item_dict) # adding group 1A so far
            item_dict['id'] = id
            ids.append(id)
            id += d_id
        else:
            ITEM_DICTS_LOOKUP[item_dict['id']] = item_dict
            ids.append(item_dict['id'])
            
    # ===================================================================================
    # 3. Step 3 is empty :)
    # ===================================================================================

    # ===================================================================================
    # 4. Lookup the IDs to split the rest of items between groups 1B and 2
    # ===================================================================================
    
    ids_for_lookup = list(ITEM_DICTS_LOOKUP.keys()) # get the ids to lookup
        
    # The total lookup is split into several because the default engine (SQLite) doesn't support huge amounts of 
    # parameters passes in a single SQL query.
    # THIS CAN BE FURTHER OPTIMIZED BY APPLYING THE "COMPRESSION" TECHNIQUES ON ARRAYS OF NUMBERS,
    # SUCH AS CONVERTING ARRAY TO RANGE IN ORDER TO MINIMIZE THE NUMBER OF SUB-LOOKUPS. 
    # TO BE DONE.
    
    DB_LOOKUP = []
    total_read = 0

    for ids_chunk in chunks(ids_for_lookup):
        n_chunk = len(ids_chunk)        
        args = [cls.__table__.c.id,]
        stmt = sql.select(args).\
            where(
                cls.__table__.c.id.in_(ids_chunk)
            ) 
        total_read += n_chunk            
        DB_LOOKUP += [id_ for id_, in session.execute(stmt)]
            
    lookup_ids = set(ITEM_DICTS_LOOKUP.keys())
    lookup_ids_1a_1b = lookup_ids-set(DB_LOOKUP)
        
    # Split the initial items into two major categories.    
    ITEM_DICTS_2 = [ITEM_DICTS_LOOKUP[id_] for id_ in DB_LOOKUP]
    ITEM_DICTS_1A_1B += [ITEM_DICTS_LOOKUP[id_] for id_ in lookup_ids_1a_1b]
    
    # Groups 1A and 1B are similar in the way data are inserted:
    #   -> need to generate item objects.    
    # In Group 2, the data already in the db.
    
    # ===================================================================================
    # 5. START ADDING ITEMS TO DATABASE
    # ===================================================================================
    
    # -- Step 5.1: create item objects for groups 1A and 1B --
            
    ###for item_dict in ITEM_DICTS_1A_1B:
    ###    
    ###   # Rename id_ to id for the insert operation
    ###   item_dict['id'] = item_dict.pop('id_')
                                                                       
    # -- Step 5.3: FINALLY ADD ITEMS !!!  --
        
    #print('ITEM_DICTS_1A_1B>>>',ITEM_DICTS_1A_1B)
                
    # ---> add new transitions (groups 1A and 1B)
    if ITEM_DICTS_1A_1B:
        session.execute(cls.__table__.insert(),ITEM_DICTS_1A_1B) 
    
    # ---> update existing transitions
    if ITEM_DICTS_2:
        # THE RIGHT WAY IS MULTIPLE WHERES, INSTEAD OF USING "AND" OPERATION!!!
        insert_values = {pname:bindparam(pname) for pname,_ in cls.__keys__}
        stmt = cls.__table__.update().\
            where(
                cls.__table__.c.id == bindparam('id')
            ).\
            values(insert_values)
        session.execute(stmt,ITEM_DICTS_2)
        
    session.commit() # COMMIT ALL CHANGES
    
    return ids

def __insert_alias_items_core__(cls,ITEM_DICTS,local=True):
    """
    Main procedure for inserting alias item dicts using core.
       cls - item class

    The idea is to split the items into 2 groups:
       GROUP 1: New items (data are inserted)
           -> GROUP 1A: New items without IDs (need to assign free IDs before inserting)
           -> GROUP 1B: New items with IDs
       GROUP 2: Existing items (always have IDs, data are updated using ID values)
    """
    
    session = VARSPACE['session']

    ITEM_DICTS_1A = [] # proxy list for group 1A (no IDs provided)
    ITEM_DICTS_1B = [] # proxy list for group 1B (IDs provided but not found in database)
    ITEM_DICTS_1A_1B = [] # proxy list for groups 1A(no IDs provided) + 1B(IDs provided but not found in database) 
    ITEM_DICTS_2 = [] # proxy list for group 2 (found in DB by IDs)
    
    ITEM_DICTS_LOOKUP = {} # proxy dict for items with IDs for the database lookup

    # ===================================================================================
    # 1. Get first available values for auto-filled parameters for the group 1A.
    # ===================================================================================
    
    id,d_id = get_first_available_(cls,'id',local)
    
    # ===================================================================================
    # 2. Loop through all items to find the group 1A and prepare the lookup table.
    # ===================================================================================

    for item_dict in ITEM_DICTS:        
        ITEM_DICTS_LOOKUP[item_dict['alias'].lower()] = item_dict
            
    #print('ITEM_DICTS_LOOKUP>>>',json.dumps(ITEM_DICTS_LOOKUP,indent=3))
            
    # ===================================================================================
    # 3. Step 3 is empty :)
    # ===================================================================================

    # ===================================================================================
    # 4. Lookup the IDs to split the rest of items between groups 1B and 2
    # ===================================================================================
    
    keys_for_lookup = list(ITEM_DICTS_LOOKUP.keys()) # get the ids to lookup
    
    # The total lookup is split into several because the default engine (SQLite) doesn't support huge amounts of 
    # parameters passes in a single SQL query.
    # THIS CAN BE FURTHER OPTIMIZED BY APPLYING THE "COMPRESSION" TECHNIQUES ON ARRAYS OF NUMBERS,
    # SUCH AS CONVERTING ARRAY TO RANGE IN ORDER TO MINIMIZE THE NUMBER OF SUB-LOOKUPS. 
    # TO BE DONE.
    
    DB_LOOKUP = []
    total_read = 0

    for vals_chunk in chunks(keys_for_lookup):
        n_chunk = len(vals_chunk)        
        #args = [
        #    cls.__table__.c.id,
        #    cls.__table__.c.alias,
        #]
        #stmt = sql.select(args).\
        stmt = sql.select([getattr(cls.__table__.c,key) for key,_ in cls.__keys__]).\
            where(
                #cls.__table__.c.alias.in_(vals_chunk)
                sql.func.lower(cls.__table__.c.alias).in_(vals_chunk)
            )
        total_read += n_chunk            
        DB_LOOKUP += session.execute(stmt)
        
    #print('stmt>>>',stmt)
    #print('DB_LOOKUP>>>',DB_LOOKUP)
                    
    # Split the initial items into two major categories.

    ids = []

    ITEM_DICTS_2 = []
    result_keys = set()
    #print('list(ITEM_DICTS_LOOKUP.keys())',ITEM_DICTS_LOOKUP.keys())
    for item_ in DB_LOOKUP:
        id_ = item_['id']
        key = item_['alias'].lower()
        item = ITEM_DICTS_LOOKUP[key]
        tmp = dict(item_); tmp.update(item); item.update(tmp) # ????
        #item.update(item_)    # DELETE
        #print('item>>>',item)
        ids.append(id_)
        ITEM_DICTS_2.append(item)
        result_keys.add(key)
            
    lookup_keys = set(ITEM_DICTS_LOOKUP.keys())
    #print('lookup_keys>>>',lookup_keys)
    #print('result_keys>>>',result_keys)
    lookup_keys_1a_1b = lookup_keys-result_keys
    #print('lookup_keys_1a_1b>>>',lookup_keys_1a_1b)

    #print('ITEM_DICTS_2 RAW>>>',ITEM_DICTS_2)
    #print('ITEM_DICTS_2>>>',json.dumps([{b:e[b] for b in e if b not in ['__parent__','__parents__']} for e in ITEM_DICTS_2],indent=3))

    ITEM_DICTS_1A_1B = []
    for key in lookup_keys_1a_1b:
        item = ITEM_DICTS_LOOKUP[key]
        item['id'] = id
        ids.append(id)
        id += 1
        ITEM_DICTS_1A_1B.append(item)
    
    #print('ITEM_DICTS_2>>>',json.dumps([{b:e[b] for b in e if b not in ['__parent__','__parents__']} for e in ITEM_DICTS_2],indent=3))

    #if not update:
    #    ITEM_DICTS_1A_1B += ITEM_DICTS_2
    #    ITEM_DICTS_2 = []
    
    # Groups 1A and 1B are similar in the way data are inserted:
    #   -> need to generate item objects.    
    # In Group 2, the data already in the db.
    
    # ===================================================================================
    # 5. START ADDING ITEMS TO DATABASE
    # ===================================================================================
    
    # -- Step 5.1: create item objects for groups 1A and 1B --
            
    ###for item_dict in ITEM_DICTS_1A_1B:
    ###    
    ###   # Rename id_ to id for the insert operation
    ###   item_dict['id'] = item_dict.pop('id_')
                                                                       
    # -- Step 5.3: FINALLY ADD ITEMS !!!  --
                
    # ---> add new transitions (groups 1A and 1B)
    if ITEM_DICTS_1A_1B:
        #print('ITEM_DICTS_1A_1B>>>',json.dumps([{b:e[b] for b in e if b not in ['__parent__','__parents__']} for e in ITEM_DICTS_1A_1B],indent=3))
        #print('ITEM_DICTS_1A_1B>>>',ITEM_DICTS_1A_1B)
        session.execute(cls.__table__.insert(),ITEM_DICTS_1A_1B) 
        
    # ---> update existing transitions
    if ITEM_DICTS_2:
        #print('ITEM_DICTS_2>>>',json.dumps([{b:e[b] for b in e if b not in ['__parent__','__parents__']} for e in ITEM_DICTS_2],indent=3))
        #print('ITEM_DICTS_2>>>',ITEM_DICTS_2)
        # THE RIGHT WAY IS MULTIPLE WHERES, INSTEAD OF USING "AND" OPERATION!!!
        insert_values = {pname:bindparam(pname) for pname,_ in cls.__keys__}
        stmt = cls.__table__.update().\
            where(
                cls.__table__.c.id == bindparam('id')
            ).\
            values(insert_values)
        session.execute(stmt,ITEM_DICTS_2)

    session.commit() # COMMIT ALL CHANGES
    
    return ids

def __update_and_commit_core__(BASE_CLS,STREAM,REFS,BACKREFS,local=True): 
    """
    # Refs and Backrefs:
    #   refs are the entities referenced by Base: 
    #       (e.g. source alias is referenced by cross-section)
    #       (e.g. molecule alias is also referenced by cross-section) 
    #   backrefs are the entities which reference Base:
    #       (e.g. molecule is backreferenced by molecule alias)
    #       (e.g. source is backreferenced by source alias)
    
    # In case of referencing there is a many-to-one relationship,
    #   while in case of backreferencing there is a one-to-many.
    
    # The type of relationship strongly affects the order of 
    #   inserting the data.   
    
    # In order to fill all IDs properly, the following order  
    #   should be maintained for the Base, its Refs, and Backrefs:
    #
    #   1) Refs are inserted into the database.
    #   2) Base is populated by the IDs from Refs, and inserted into the database.
    #   3) Backrefs are populated by the IDs from Base, and inserted into the database.
    """
    
    refs_flag = True if len(REFS)>0 else False
    backrefs_flag = True if len(BACKREFS)>0 else False
        
    BASE_ITEMS = list(STREAM) # not efficient if number of items is very large (i.e. transitions)
    db_backend = VARSPACE['db_backend']
        
    for ref in REFS:
        dct = REFS[ref]
        dct['items_dict'] = {}
        #dct['items_list'] = []

    for backref in BACKREFS:
        dct = BACKREFS[backref]
        dct['items_dict'] = {}
        #dct['items_list'] = []

    #for item in BASE_ITEMS:
    for nitem,item in enumerate(BASE_ITEMS):
        
        # collect refs items
        #for ref in REFS:
        for nref,ref in enumerate(REFS):
            dct = REFS[ref]
            ref_item = item[ref]
            if type(ref_item) is str: # convert to dict, if given in "short" form
                ref_item = {'alias':ref_item,'__nref__':nref,'__nitem__':nitem}
            alias = ref_item['alias']
            if alias in dct['items_dict']:
                ref_item = dct['items_dict'][alias]
            if '__parents__' not in item:
                item['__parents__'] = {ref:ref_item}
            else:
                item['__parents__'][ref] = ref_item
            dct['items_dict'][ref_item['alias']] = ref_item
            #dct['items_list'].append(ref_item)
        
        # collect backrefs items
        for backref in BACKREFS:
            dct = BACKREFS[backref]
            for backref_item in item[backref]:
                backref_item['__parent__'] = item
                dct['items_dict'][backref_item['alias']] = backref_item
                #dct['items_list'].append(backref_item)
                
    if refs_flag:                    
        
        # insert REFS_ITEMS into the database
        for ref in REFS:
            dct = REFS[ref]
            #print('dct[items_dict] BEFORE>>>',dct['items_dict'])
            __insert_alias_items_core__(cls=getattr(db_backend.models,dct['class']),
                #ITEM_DICTS=dct['items_list'],local=local)    
                ITEM_DICTS=[dct['items_dict'][r] for r in dct['items_dict']],local=local)    
            #print('dct[items_dict] AFTER>>>',dct['items_dict'])
                
        # connect BASE_ITEMS with items REFS_ITEMS
        for item in BASE_ITEMS:
            #print('item[__parents__]>>>',item['__parents__'])
            #print('item>>>',item)
            #print('dct[items_list]>>>',dct['items_list'])
            for ref in item['__parents__']:
                dct = REFS[ref]
                ref_item = item['__parents__'][ref]                
                for base_field,ref_field in dct['join']:
                    item[base_field] = ref_item[ref_field]
            del item['__parents__']
    
    # insert BASE_ITEMS into database
    ids_base = __insert_base_items_core__(cls=BASE_CLS,
        ITEM_DICTS=BASE_ITEMS,local=local)

    if backrefs_flag:        

        # connect BACKREFS_ITEMS with items from BASE_ITEMS
        for backref in BACKREFS: 
            dct = BACKREFS[backref]
            #for backref_item in dct['items_list']:
            for alias in dct['items_dict']:
                backref_item = dct['items_dict'][alias]
                item = backref_item['__parent__']
                for base_field,ref_field in dct['join']:
                    backref_item[ref_field] = item[base_field]                    
                del backref_item['__parent__']
    
        # insert BACKREFS_ITEMS into the database
        for backref in BACKREFS:
            dct = BACKREFS[backref]
            __insert_alias_items_core__(cls=getattr(db_backend.models,dct['class']),
                #ITEM_DICTS=dct['items_list'],local=local)
                ITEM_DICTS=[dct['items_dict'][r] for r in dct['items_dict']],local=local)
            
    #return ids_base
    return query(BASE_CLS).filter(BASE_CLS.id.in_(ids_base))
