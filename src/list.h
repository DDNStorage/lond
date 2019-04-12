/* defines LIST functions */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#ifndef _LOND_LIST_H_
#define _LOND_LIST_H_

#include <stdlib.h> /* For NULL */

/*
 * Simple doubly linked list implementation.
 *
 * Some of the internal functions ("__xxx") are useful when
 * manipulating whole lists rather than single entries, as
 * sometimes we already know the next/prev entries and we can
 * generate better code by using them directly rather than
 * using the generic single-entry routines.
 */

#define prefetch(a) ((void)a)

struct lond_list_head {
	struct lond_list_head *next, *prev;
};


#define LOND_LIST_HEAD_INIT(name) { &(name), &(name) }

#define LOND_LIST_HEAD(name) \
	struct lond_list_head name = LOND_LIST_HEAD_INIT(name)

#define LOND_INIT_LIST_HEAD(ptr) do { \
	(ptr)->next = (ptr); (ptr)->prev = (ptr); \
} while (0)

/**
 * Insert a new entry between two known consecutive entries.
 *
 * This is only for internal list manipulation where we know
 * the prev/next entries already!
 */
static inline void __lond_list_add(struct lond_list_head *new,
				   struct lond_list_head *prev,
				   struct lond_list_head *next)
{
	next->prev = new;
	new->next = next;
	new->prev = prev;
	prev->next = new;
}

/**
 * Insert an entry at the start of a list.
 * \param new  new entry to be inserted
 * \param head list to add it to
 *
 * Insert a new entry after the specified head.
 * This is good for implementing stacks.
 */
static inline void lond_list_add(struct lond_list_head *new,
				 struct lond_list_head *head)
{
	__lond_list_add(new, head, head->next);
}

/**
 * Insert an entry at the end of a list.
 * \param new  new entry to be inserted
 * \param head list to add it to
 *
 * Insert a new entry before the specified head.
 * This is useful for implementing queues.
 */
static inline void lond_list_add_tail(struct lond_list_head *new,
				      struct lond_list_head *head)
{
	__lond_list_add(new, head->prev, head);
}

/*
 * Delete a list entry by making the prev/next entries
 * point to each other.
 *
 * This is only for internal list manipulation where we know
 * the prev/next entries already!
 */
static inline void __lond_list_del(struct lond_list_head *prev,
				   struct lond_list_head *next)
{
	next->prev = prev;
	prev->next = next;
}

/**
 * Remove an entry from the list it is currently in.
 * \param entry the entry to remove
 * Note: lond_list_empty(entry) does not return true after this, the entry is in an
 * undefined state.
 */
static inline void lond_list_del(struct lond_list_head *entry)
{
	__lond_list_del(entry->prev, entry->next);
}

/**
 * Remove an entry from the list it is currently in and reinitialize it.
 * \param entry the entry to remove.
 */
static inline void lond_list_del_init(struct lond_list_head *entry)
{
	__lond_list_del(entry->prev, entry->next);
	LOND_INIT_LIST_HEAD(entry);
}

/**
 * Remove an entry from the list it is currently in and insert it at the start
 * of another list.
 * \param list the entry to move
 * \param head the list to move it to
 */
static inline void list_move(struct lond_list_head *list,
			     struct lond_list_head *head)
{
	__lond_list_del(list->prev, list->next);
	lond_list_add(list, head);
}

/**
 * Remove an entry from the list it is currently in and insert it at the end of
 * another list.
 * \param list the entry to move
 * \param head the list to move it to
 */
static inline void lond_list_move_tail(struct lond_list_head *list,
				       struct lond_list_head *head)
{
	__lond_list_del(list->prev, list->next);
	lond_list_add_tail(list, head);
}

/**
 * Test whether a list is empty
 * \param head the list to test.
 */
static inline int lond_list_empty(const struct lond_list_head *head)
{
	return head->next == head;
}

/**
 * Test whether a list is empty and not being modified
 * \param head the list to test
 *
 * Tests whether a list is empty _and_ checks that no other CPU might be
 * in the process of modifying either member (next or prev)
 *
 * NOTE: using lond_list_empty_careful() without synchronization
 * can only be safe if the only activity that can happen
 * to the list entry is lond_list_del_init(). Eg. it cannot be used
 * if another CPU could re-list_add() it.
 */
static inline int lond_list_empty_careful(const struct lond_list_head *head)
{
	struct lond_list_head *next = head->next;

	return (next == head) && (next == head->prev);
}

/**
 * lond_list_is_singular - tests whether a list has just one entry.
 * @head: the list to test.
 */
static inline int lond_list_is_singular(const struct lond_list_head *head)
{
	return !lond_list_empty(head) && (head->next == head->prev);
}

static inline void __lond_list_cut_position(struct lond_list_head *list,
		struct lond_list_head *head, struct lond_list_head *entry)
{
	struct lond_list_head *new_first = entry->next;

	list->next = head->next;
	list->next->prev = list;
	list->prev = entry;
	entry->next = list;
	head->next = new_first;
	new_first->prev = head;
}

/**
 * lond_list_cut_position - cut a list into two
 * @list: a new list to add all removed entries
 * @head: a list with entries
 * @entry: an entry within head, could be the head itself
 *	and if so we won't cut the list
 *
 * This helper moves the initial part of @head, up to and
 * including @entry, from @head to @list. You should
 * pass on @entry an element you know is on @head. @list
 * should be an empty list or a list you do not care about
 * losing its data.
 *
 */
static inline void lond_list_cut_position(struct lond_list_head *list,
		struct lond_list_head *head, struct lond_list_head *entry)
{
	if (lond_list_empty(head))
		return;
	if (lond_list_is_singular(head) &&
		(head->next != entry && head != entry))
		return;
	if (entry == head)
		LOND_INIT_LIST_HEAD(list);
	else
		__lond_list_cut_position(list, head, entry);
}

static inline void __lond_list_splice(struct lond_list_head *list,
				      struct lond_list_head *head)
{
	struct lond_list_head *first = list->next;
	struct lond_list_head *last = list->prev;
	struct lond_list_head *at = head->next;

	first->prev = head;
	head->next = first;

	last->next = at;
	at->prev = last;
}

/**
 * Join two lists
 * \param list the new list to add.
 * \param head the place to add it in the first list.
 *
 * The contents of \a list are added at the start of \a head.  \a list is in an
 * undefined state on return.
 */
static inline void lond_list_splice(struct lond_list_head *list,
				    struct lond_list_head *head)
{
	if (!lond_list_empty(list))
		__lond_list_splice(list, head);
}

/**
 * Join two lists and reinitialise the emptied list.
 * \param list the new list to add.
 * \param head the place to add it in the first list.
 *
 * The contents of \a list are added at the start of \a head.  \a list is empty
 * on return.
 */
static inline void lond_list_splice_init(struct lond_list_head *list,
					 struct lond_list_head *head)
{
	if (!lond_list_empty(list)) {
		__lond_list_splice(list, head);
		LOND_INIT_LIST_HEAD(list);
	}
}

/**
 * Get the container of a list
 * \param ptr	 the embedded list.
 * \param type	 the type of the struct this is embedded in.
 * \param member the member name of the list within the struct.
 */
#define lond_list_entry(ptr, type, member) \
	((type *)((char *)(ptr)-(char *)(&((type *)0)->member)))

/**
 * Iterate over a list
 * \param pos	the iterator
 * \param head	the list to iterate over
 *
 * Behaviour is undefined if \a pos is removed from the list in the body of the
 * loop.
 */
#define lond_list_for_each(pos, head) \
	for (pos = (head)->next, prefetch(pos->next); pos != (head); \
		pos = pos->next, prefetch(pos->next))

/**
 * Iterate over a list safely
 * \param pos	the iterator
 * \param n     temporary storage
 * \param head	the list to iterate over
 *
 * This is safe to use if \a pos could be removed from the list in the body of
 * the loop.
 */
#define lond_list_for_each_safe(pos, n, head) \
	for (pos = (head)->next, n = pos->next; pos != (head); \
		pos = n, n = pos->next)

/**
 * Iterate over a list continuing after existing point
 * \param pos    the type * to use as a loop counter
 * \param head   the list head
 * \param member the name of the list_struct within the struct
 */
#define lond_list_for_each_entry_continue(pos, head, member)                 \
	for (pos = lond_list_entry(pos->member.next, typeof(*pos), member);  \
	     prefetch(pos->member.next), &pos->member != (head);	     \
	     pos = lond_list_entry(pos->member.next, typeof(*pos), member))

/**
 * \defgroup hlist Hash List
 * Double linked lists with a single pointer list head.
 * Mostly useful for hash tables where the two pointer list head is too
 * wasteful.  You lose the ability to access the tail in O(1).
 * @{
 */

typedef struct hlist_node {
	struct hlist_node *next, **pprev;
} hlist_node_t;

typedef struct hlist_head {
	hlist_node_t *first;
} hlist_head_t;

/* @} */

/*
 * "NULL" might not be defined at this point
 */
#ifdef NULL
#define NULL_P NULL
#else
#define NULL_P ((void *)0)
#endif

/**
 * \addtogroup hlist
 * @{
 */

#define HLIST_HEAD_INIT { NULL_P }
#define HLIST_HEAD(name) hlist_head_t name = { NULL_P }
#define INIT_HLIST_HEAD(ptr) ((ptr)->first = NULL_P)
#define INIT_HLIST_NODE(ptr) ((ptr)->next = NULL_P, (ptr)->pprev = NULL_P)

static inline int hlist_unhashed(const hlist_node_t *h)
{
	return !h->pprev;
}

static inline int hlond_list_empty(const hlist_head_t *h)
{
	return !h->first;
}

static inline void __hlist_del(hlist_node_t *n)
{
	hlist_node_t *next = n->next;
	hlist_node_t **pprev = n->pprev;
	*pprev = next;
	if (next)
		next->pprev = pprev;
}

static inline void hlist_del(hlist_node_t *n)
{
	__hlist_del(n);
}

static inline void hlist_del_init(hlist_node_t *n)
{
	if (n->pprev)  {
		__hlist_del(n);
		INIT_HLIST_NODE(n);
	}
}

static inline void hlist_add_head(hlist_node_t *n,
				  hlist_head_t *h)
{
	hlist_node_t *first = h->first;

	n->next = first;
	if (first)
		first->pprev = &n->next;
	h->first = n;
	n->pprev = &h->first;
}

/* next must be != NULL */
static inline void hlist_add_before(hlist_node_t *n,
					hlist_node_t *next)
{
	n->pprev = next->pprev;
	n->next = next;
	next->pprev = &n->next;
	*(n->pprev) = n;
}

static inline void hlist_add_after(hlist_node_t *n,
				   hlist_node_t *next)
{
	next->next = n->next;
	n->next = next;
	next->pprev = &n->next;

	if (next->next)
		next->next->pprev  = &next->next;
}

#define hlist_entry(ptr, type, member) container_of(ptr, type, member)

#define hlist_for_each(pos, head) \
	for (pos = (head)->first; pos && (prefetch(pos->next), 1); \
	     pos = pos->next)

#define hlist_for_each_safe(pos, n, head) \
	for (pos = (head)->first; pos && (n = pos->next, 1); \
	     pos = n)

/**
 * Iterate over an hlist of given type
 * \param tpos	 the type * to use as a loop counter.
 * \param pos	 the &struct hlist_node to use as a loop counter.
 * \param head	 the head for your list.
 * \param member the name of the hlist_node within the struct.
 */
#define hlist_for_each_entry(tpos, pos, head, member)                 \
	for (pos = (head)->first;                                         \
	     pos && ({ prefetch(pos->next); 1; }) &&                       \
		({ tpos = hlist_entry(pos, typeof(*tpos), member); 1; });      \
	     pos = pos->next)

/**
 * Iterate over an hlist continuing after existing point
 * \param tpos	 the type * to use as a loop counter.
 * \param pos	 the &struct hlist_node to use as a loop counter.
 * \param member the name of the hlist_node within the struct.
 */
#define hlist_for_each_entry_continue(tpos, pos, member)              \
	for (pos = (pos)->next;                                           \
	     pos && ({ prefetch(pos->next); 1; }) &&                       \
		({ tpos = hlist_entry(pos, typeof(*tpos), member); 1; });      \
	     pos = pos->next)

/**
 * Iterate over an hlist continuing from an existing point
 * \param tpos	 the type * to use as a loop counter.
 * \param pos	 the &struct hlist_node to use as a loop counter.
 * \param member the name of the hlist_node within the struct.
 */
#define hlist_for_each_entry_from(tpos, pos, member)			      \
	for (; pos && ({ prefetch(pos->next); 1; }) &&                     \
		({ tpos = hlist_entry(pos, typeof(*tpos), member); 1; });      \
	     pos = pos->next)

/**
 * Iterate over an hlist of given type safe against removal of list entry
 * \param tpos	 the type * to use as a loop counter.
 * \param pos	 the &struct hlist_node to use as a loop counter.
 * \param n	 another &struct hlist_node to use as temporary storage
 * \param head	 the head for your list.
 * \param member the name of the hlist_node within the struct.
 */
#define hlist_for_each_entry_safe(tpos, pos, n, head, member)         \
	for (pos = (head)->first;                                         \
	     pos && ({ n = pos->next; 1; }) &&                            \
		({ tpos = hlist_entry(pos, typeof(*tpos), member); 1; });      \
	     pos = n)

/* @} */

#ifndef lond_list_for_each_prev
/**
 * Iterate over a list in reverse order
 * \param pos	the &struct lond_list_head to use as a loop counter.
 * \param head	the head for your list.
 */
#define lond_list_for_each_prev(pos, head) \
	for (pos = (head)->prev, prefetch(pos->prev); pos != (head);      \
		pos = pos->prev, prefetch(pos->prev))

#endif /* list_for_each_prev */

#ifndef lond_list_for_each_entry
/**
 * Iterate over a list of given type
 * \param pos        the type * to use as a loop counter.
 * \param head       the head for your list.
 * \param member     the name of the list_struct within the struct.
 */
#define lond_list_for_each_entry(pos, head, member)                        \
	for (pos = lond_list_entry((head)->next, typeof(*pos), member),    \
		     prefetch(pos->member.next);                              \
	     &pos->member != (head);                                      \
	     pos = lond_list_entry(pos->member.next, typeof(*pos), member),    \
	     prefetch(pos->member.next))
#endif /* list_for_each_entry */

#ifndef lond_list_for_each_entry_rcu
#define list_for_each_entry_rcu(pos, head, member) \
	list_for_each_entry(pos, head, member)
#endif

#ifndef lond_list_for_each_entry_rcu
#define list_for_each_entry_rcu(pos, head, member) \
	list_for_each_entry(pos, head, member)
#endif

#ifndef lond_list_for_each_entry_reverse
/**
 * Iterate backwards over a list of given type.
 * \param pos        the type * to use as a loop counter.
 * \param head       the head for your list.
 * \param member     the name of the list_struct within the struct.
 */
#define list_for_each_entry_reverse(pos, head, member)                \
	for (pos = lond_list_entry((head)->prev, typeof(*pos), member); \
	     prefetch(pos->member.prev), &pos->member != (head);          \
	     pos = lond_list_entry(pos->member.prev, typeof(*pos), member))
#endif /* list_for_each_entry_reverse */

#ifndef lond_list_for_each_entry_safe
/**
 * Iterate over a list of given type safe against removal of list entry
 * \param pos        the type * to use as a loop counter.
 * \param n          another type * to use as temporary storage
 * \param head       the head for your list.
 * \param member     the name of the list_struct within the struct.
 */
#define lond_list_for_each_entry_safe(pos, n, head, member)                \
	for (pos = lond_list_entry((head)->next, typeof(*pos), member),    \
		n = lond_list_entry(pos->member.next, typeof(*pos), member); \
	     &pos->member != (head);                                      \
	     pos = n, n = lond_list_entry(n->member.next, typeof(*n), member))

#endif /* lond_list_for_each_entry_safe */

#ifndef lond_list_for_each_entry_safe_from
/**
 * Iterate over a list continuing from an existing point
 * \param pos        the type * to use as a loop cursor.
 * \param n          another type * to use as temporary storage
 * \param head       the head for your list.
 * \param member     the name of the list_struct within the struct.
 *
 * Iterate over list of given type from current point, safe against
 * removal of list entry.
 */
#define lond_list_for_each_entry_safe_from(pos, n, head, member)               \
	for (n = lond_list_entry(pos->member.next, typeof(*pos), member);      \
	     &pos->member != (head);                                      \
	     pos = n, n = lond_list_entry(n->member.next, typeof(*n), member))
#endif /* lond_list_for_each_entry_safe_from */

#define lond_list_for_each_entry_typed(pos, head, type, member)		  \
	for (pos = lond_list_entry((head)->next, type, member),		  \
		     prefetch(pos->member.next);                          \
	     &pos->member != (head);                                  \
	     pos = lond_list_entry(pos->member.next, type, member),	      \
	     prefetch(pos->member.next))

#define lond_list_for_each_entry_reverse_typed(pos, head, type, member) \
	for (pos = lond_list_entry((head)->prev, type, member);		       \
	     prefetch(pos->member.prev), &pos->member != (head);	   \
	     pos = lond_list_entry(pos->member.prev, type, member))

#define lond_list_for_each_entry_safe_typed(pos, n, head, type, member)	  \
	for (pos = lond_list_entry((head)->next, type, member),		      \
	     n = lond_list_entry(pos->member.next, type, member);	      \
	     &pos->member != (head);                                  \
	     pos = n, n = lond_list_entry(n->member.next, type, member))

#define lond_list_for_each_entry_safe_from_typed(pos, n, head, type, member)   \
	for (n = lond_list_entry(pos->member.next, type, member);              \
	     &pos->member != (head);                                      \
	     pos = n, n = lond_list_entry(n->member.next, type, member))

#define hlist_for_each_entry_typed(tpos, pos, head, type, member)     \
	for (pos = (head)->first;                                         \
	     pos && (prefetch(pos->next), 1) &&                           \
		(tpos = hlist_entry(pos, type, member), 1);                   \
	     pos = pos->next)

#define hlist_for_each_entry_safe_typed(tpos, pos, n, head, type, member) \
	for (pos = (head)->first;                                             \
	     pos && (n = pos->next, 1) &&                                     \
		(tpos = hlist_entry(pos, type, member), 1);                   \
	     pos = n)

/**
 * lond_list_sort - sort a list.
 * @priv: private data, passed to @cmp
 * @head: the list to sort
 * @cmp: the elements comparison function
 *
 * This function has been implemented by Mark J Roberts <mjr@znex.org>. It
 * implements "merge sort" which has O(nlog(n)) complexity. The list is sorted
 * in ascending order.
 *
 * The comparison function @cmp is supposed to return a negative value if @a is
 * than @b, and a positive value if @a is greater than @b. If @a and @b are
 * equivalent, then it does not matter what this function returns.
 */
static inline void lond_list_sort(void *priv, struct lond_list_head *head,
				  int (*cmp)(void *priv,
				  struct lond_list_head *a,
				  struct lond_list_head *b))
{
	struct lond_list_head *p, *q, *e, *list, *tail, *oldhead;
	int insize, nmerges, psize, qsize, i;

	if (lond_list_empty(head))
		return;

	list = head->next;
	lond_list_del(head);
	insize = 1;
	for (;;) {
		p = oldhead = list;
		list = tail = NULL;
		nmerges = 0;

		while (p) {
			nmerges++;
			q = p;
			psize = 0;
			for (i = 0; i < insize; i++) {
				psize++;
				q = q->next == oldhead ? NULL : q->next;
				if (!q)
					break;
			}

			qsize = insize;
			while (psize > 0 || (qsize > 0 && q)) {
				if (!psize) {
					e = q;
					q = q->next;
					qsize--;
					if (q == oldhead)
						q = NULL;
				} else if (!qsize || !q) {
					e = p;
					p = p->next;
					psize--;
					if (p == oldhead)
						p = NULL;
				} else if (cmp(priv, p, q) <= 0) {
					e = p;
					p = p->next;
					psize--;
					if (p == oldhead)
						p = NULL;
				} else {
					e = q;
					q = q->next;
					qsize--;
					if (q == oldhead)
						q = NULL;
				}
				if (tail)
					tail->next = e;
				else
					list = e;
				e->prev = tail;
				tail = e;
			}
			p = q;
		}

		tail->next = list;
		list->prev = tail;

		if (nmerges <= 1)
			break;

		insize *= 2;
	}

	head->next = list;
	head->prev = list->prev;
	list->prev->next = head;
	list->prev = head;
}

#endif /* _LOND_LIST_H_ */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
