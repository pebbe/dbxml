#ifndef C_DBXML_H
#define C_DBXML_H

#ifdef __cplusplus
extern "C" {
#endif

    typedef struct c_dbxml_t *c_dbxml;

    typedef struct c_dbxml_docs_t *c_dbxml_docs;

    c_dbxml c_dbxml_open(char const *filename);
    void c_dbxml_free(c_dbxml db);

    int c_dbxml_error(c_dbxml db);
    char const * c_dbxml_errstring(c_dbxml db);

    /**** WRITE ****/

    /* replace if replace != 0
     * return 0 bij fout
     */
    int c_dbxml_put_file(c_dbxml db, char const *filename, int replace);

    /* replace if replace != 0
     * return 0 bij fout
     */
    int c_dbxml_put_xml(c_dbxml db, char const *name, char const *data, int replace);

    /* replace if replace != 0
     * return 0 bij fout
     */
    int c_dbxml_merge(c_dbxml db, char const *dbxmlfile, int replace);

    /* return 0 bij fout */
    int c_dbxml_remove(c_dbxml db, char const *name);

    /**** READ ****/

    char const * c_dbxml_get(c_dbxml db, char const * name);

    unsigned long long c_dbxml_size(c_dbxml db);

    c_dbxml_docs c_dbxml_get_all(c_dbxml db);
    c_dbxml_docs c_dbxml_get_query(c_dbxml db, char const *query);
    int c_dbxml_docs_next(c_dbxml_docs docs);
    char const * c_dbxml_docs_name(c_dbxml_docs docs);
    char const * c_dbxml_docs_content(c_dbxml_docs docs);
    void c_dbxml_docs_free(c_dbxml_docs docs);

#ifdef __cplusplus
}
#endif

#endif /* C_DBXML_H */
