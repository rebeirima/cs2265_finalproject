#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <getopt.h>
#include <float.h>
#include <stdlib.h>
#include <limits.h>
#include <math.h>

#include <gsl/gsl_rng.h>
#include <gsl/gsl_randist.h>

#include "data_types.h"
#include "logo.h"

/////////////////////////////////////////////////////////////////////////////
///////////////             D E F I N I T I O N S             ///////////////
/////////////////////////////////////////////////////////////////////////////
// This is used for storing gets existing in the workload so far
// They are then used for skewed query workload generation
#define MAX_OLD_GETS_POOL_SIZE 10000000 // ~= 80MB

// This is used for storing puts existing issued so far
// They are then used for non empty get query generation
#define MAX_OLD_PUTS_POOL_SIZE 10000000 // ~= 80MB

// Constants for the different operations generated
#define OPERATION_PUT 0
#define OPERATION_GET 1
#define OPERATION_RANGE 2
#define OPERATION_DELETE 3

// This is the product name
#define PRODUCT "\
 -------------------------------------------------------------------------\n\
                     W O R K L O A D   G E N E R A T O R                  \n\
 -------------------------------------------------------------------------\n\
 "
/////////////////////////////////////////////////////////////////////////////

struct settings
{
    int puts;
    int gets;
    float gets_skewness;
    float gets_misses_ratio;
    int ranges;
    char uniform_ranges; /* true (uniform) or false (gaussian) */
    int deletes;
    int external_puts;
    int seed;
    int min_lookup_batch_size;
    int max_lookup_batch_size;
    int min_update_batch_size;
    int max_update_batch_size;
};

/**
 * Default values for settings
 */
void initialize_settings(struct settings *s)
{
    s->puts = 10000000;
    s->gets = 10000000;
    s->gets_skewness = 0;
    s->gets_misses_ratio = 0.5;
    s->ranges = 0;
    s->deletes = 0;
    s->external_puts = 0;
    s->seed = 13141;
    s->uniform_ranges = 1;
}

// ====================================================================== //
//                         MAIN WORKLOAD GENERATOR
// ====================================================================== //
/**
 * Generates a workload based on some settings
 */
void generate_workload(struct settings *s)
{
    int i, j;

    /////////////////////////////////////
    ///    INITIALIZE NUMBER POOLS    ///
    /////////////////////////////////////
    // Initialize random generator
    gsl_rng_default_seed = s->seed;
    const gsl_rng_type *T;
    gsl_rng *r;
    gsl_rng_env_setup();
    T = gsl_rng_default;
    r = gsl_rng_alloc(T);

    // Buffer of previous puts (for non empty gets)
    int old_puts_pool_max_size;
    int old_puts_pool_count = 0;
    if (s->puts < MAX_OLD_PUTS_POOL_SIZE)
    {
        old_puts_pool_max_size = s->puts * sizeof(KEY_t);
    }
    else
    {
        old_puts_pool_max_size = MAX_OLD_PUTS_POOL_SIZE * sizeof(KEY_t);
    }
    KEY_t *old_puts_pool = malloc(sizeof(KEY_t) * old_puts_pool_max_size);
    for (i = 0; i < old_puts_pool_max_size; i++)
    {
        old_puts_pool[i] = 0;
    }

    // Buffer of previous gets (for skewed workloads)
    int old_gets_pool_max_size;
    int old_gets_pool_count = 0;
    if (s->gets < MAX_OLD_GETS_POOL_SIZE)
    {
        old_gets_pool_max_size = s->gets * sizeof(KEY_t);
    }
    else
    {
        old_gets_pool_max_size = MAX_OLD_GETS_POOL_SIZE * sizeof(KEY_t);
    }
    KEY_t *old_gets_pool = malloc(sizeof(KEY_t) * old_gets_pool_max_size);
    for (i = 0; i < old_gets_pool_max_size; i++)
    {
        old_gets_pool[i] = 0;
    }

    /////////////////////////////////////
    //             PROCESS             //
    /////////////////////////////////////
    int current_deletes = 0;
    int current_puts = 0;
    int current_gets = 0;
    int current_ranges = 0;
    int prev_operation = -1;
    int current_file = 0;
    FILE *pf = NULL;

    int op_count = 0;
    int operation = OPERATION_PUT;
    while (op_count < 20000000)
    {
        // TODO: Check if this operation has remaining else continue.
        switch (operation)
        {
        case OPERATION_PUT:
            if (current_puts >= s->puts)
                continue;
            break;
        case OPERATION_GET:
            if (current_gets >= s->gets)
                continue;
            break;
        case OPERATION_RANGE:
            if (current_ranges >= s->ranges)
                continue;
            break;
        case OPERATION_DELETE:
            if (current_deletes >= s->deletes)
                continue;
            break;
        default:
            break;
        }

        // Check if I have to open a new binary file for puts
        if (prev_operation != OPERATION_PUT &&
            operation == OPERATION_PUT &&
            s->external_puts)
        {
            if (pf != NULL)
            {
                fclose(pf);
                pf = NULL;
            }
            char pfname[256];
            sprintf(pfname, "%d.dat", current_file++);
            pf = fopen(pfname, "wb");
            printf("l \"%s\"\n", pfname);
        }
        prev_operation = operation;

        // Perform operation
        if (operation == OPERATION_PUT)
        {
            ////////////////// PUTS //////////////////
            // Pick key and value
            KEY_t k = GEN_RANDOM_KEY_UNIFORM(r);
            VAL_t v = GEN_RANDOM_VAL_UNIFORM(r);

            // Write
            if (s->external_puts)
            {
                fwrite(&k, sizeof(KEY_t), 1, pf);
                fwrite(&v, sizeof(VAL_t), 1, pf);
            }
            else
            {
                printf(PUT_PATTERN, k, v);
            }

            // Store this number in the pool for
            // choosing it for future get queries
            if (old_puts_pool_count >= old_puts_pool_max_size)
            {
                old_puts_pool[rand() % old_puts_pool_count] = k;
            }
            else
            {
                old_puts_pool[old_puts_pool_count++] = k;
            }

            current_puts++;
        }
        else if (operation == OPERATION_GET)
        {
            // No puts yet
            if (!current_puts)
                continue;

            ////////////////// GETS //////////////////
            KEY_t k;
            // With a probability use a new key as a query
            if ((rand() % 10) > (s->gets_skewness * 10) ||
                old_gets_pool_count == 0)
            {
                // With a probability use one of the
                // previously inserted data as a query
                if ((rand() % 10) > (s->gets_misses_ratio * 10))
                {
                    k = old_puts_pool[rand() % old_puts_pool_count];
                }
                // Or issue a completely random query
                // most probably causing a miss
                else
                {
                    k = GEN_RANDOM_KEY_UNIFORM(r);
                }
                // Store this number in the pool for
                // choosing it for future skewed queries
                if (old_gets_pool_count >= old_gets_pool_max_size)
                {
                    old_gets_pool[rand() % old_gets_pool_count] = k;
                }
                else
                {
                    old_gets_pool[old_gets_pool_count++] = k;
                }
            }
            // Or use one of the previously queried keys
            else
            {
                k = old_gets_pool[rand() % old_gets_pool_count];
            }

            // Write in output
            printf(GET_PATTERN, k);

            current_gets++;
        }
        else if (operation == OPERATION_RANGE)
        {
            // No puts yet
            if (!current_puts)
                continue;

            //////////////// RANGES //////////////////
            // TODO: USE DIFFERENT DISTRIBUTIONS
            KEY_t a;
            KEY_t b;
            if (s->uniform_ranges)
            {
                a = GEN_RANDOM_KEY_UNIFORM(r);
                b = GEN_RANDOM_KEY_UNIFORM(r);
            }
            else
            {
                a = GEN_RANDOM_KEY_GAUSS(r);
                b = GEN_RANDOM_KEY_GAUSS(r);
            }

            if (a < b)
            {
                printf(RANGE_PATTERN, a, b);
            }
            else
            {
                printf(RANGE_PATTERN, b, a);
            }

            current_ranges++;
        }
        else if (operation == OPERATION_DELETE)
        {
            // No puts yet
            if (!current_puts)
                continue;

            ////////////////// DELETES //////////////////
            KEY_t d = old_puts_pool[rand() % old_puts_pool_count];
            printf(DELETE_PATTERN, d);

            current_deletes++;
        }

        op_count += 1;
        if (op_count % 10000 == 0)
        {
            operation = 1 - operation;
        }
    }

    // Free memory
    free(old_gets_pool);
    free(old_puts_pool);
    gsl_rng_free(r);

    // Close file
    if (pf != NULL)
    {
        fclose(pf);
        pf = NULL;
    }
}

/**
 * The main entry point to the software.
 */
int main(int argc, char **argv)
{
    struct settings s;
    initialize_settings(&s);
    generate_workload(&s);

    // Done!
    exit(EXIT_SUCCESS);
}
