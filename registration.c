/*
 * =============================================================================
 * UNIVERSITY COURSE REGISTRATION SYSTEM
 * =============================================================================
 * File        : registration.c
 * Description : A concurrent university course registration simulation using
 *               POSIX threads (pthreads) in C. Students are modeled as
 *               concurrent threads competing for shared course seats, protected
 *               using mutex locks. Priority handling gives preference to
 *               final-year (high-priority) students while preventing starvation.
 * 
 * OS Concepts : Threads, Mutex Locks, Semaphores, Critical Sections,
 *               Priority Handling, Deadlock Prevention, Starvation Avoidance
 * 
 * Compile     : gcc -o registration registration.c -lpthread
 * Run         : ./registration
 * =============================================================================
 * Group Members:
 *   Member 1  : Data structures, course logic, correctness rules
 *   Member 2  : Thread creation, synchronization, mutex/semaphore logic
 *   Member 3  : Priority handling, logging, testing
 * =============================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <time.h>
#include <stdarg.h>

/* =============================================================================
 * CONSTANTS AND CONFIGURATION
 * =============================================================================
 */
#define MAX_COURSES         10
#define MAX_STUDENTS        200
#define MAX_COURSES_PER_STU 3
#define COURSE_NAME_LEN     50
#define LOG_BUFFER_SIZE     512

/* Priority Levels */
#define PRIORITY_HIGH       1   /* Final-year / graduating students */
#define PRIORITY_LOW        0   /* Regular students */

/* Colors for terminal output */
#define COLOR_RESET         "\033[0m"
#define COLOR_RED           "\033[1;31m"
#define COLOR_GREEN         "\033[1;32m"
#define COLOR_YELLOW        "\033[1;33m"
#define COLOR_BLUE          "\033[1;34m"
#define COLOR_MAGENTA       "\033[1;35m"
#define COLOR_CYAN          "\033[1;36m"
#define COLOR_WHITE         "\033[1;37m"
#define COLOR_BOLD          "\033[1m"
#define COLOR_BG_BLUE       "\033[44m"
#define COLOR_BG_GREEN      "\033[42m"

/* =============================================================================
 * MODULE 1: DATA STRUCTURES
 * =============================================================================
 */

/*
 * Function    : Course structure
 * Description : Represents a university course with ID, name, total capacity,
 *               available seats, and a dedicated mutex lock for thread-safe
 *               seat management. Each course has its own mutex to allow
 *               concurrent access to different courses simultaneously.
 */
typedef struct {
    int     course_id;                   /* Unique course identifier */
    char    course_name[COURSE_NAME_LEN];/* Human-readable course name */
    int     total_seats;                 /* Total seats in the course */
    int     available_seats;            /* Remaining available seats */
    int     registered_count;           /* Number of students registered */
    pthread_mutex_t seat_mutex;         /* Per-course mutex for atomicity */
} Course;

/*
 * Function    : StudentRequest structure
 * Description : Represents a student's registration request, containing
 *               the student's ID, name, priority level, list of courses
 *               they wish to register for, and their registration outcomes.
 */
typedef struct {
    int     student_id;                        /* Unique student identifier */
    char    student_name[50];                  /* Student name */
    int     priority;                          /* PRIORITY_HIGH or PRIORITY_LOW */
    int     requested_courses[MAX_COURSES_PER_STU]; /* Course IDs to register for */
    int     num_courses;                        /* Number of course requests */
    int     success_count;                     /* Successful registrations */
    int     fail_count;                        /* Failed registrations */
} StudentRequest;

/*
 * Function    : ThreadArgs structure
 * Description : Arguments passed to each student thread, bundling the
 *               student request pointer with a pointer to the course array.
 */
typedef struct {
    StudentRequest *student;
    Course         *courses;
    int             num_courses;
} ThreadArgs;

/*
 * Function    : RegistrationLog structure
 * Description : Stores a single log entry for a registration attempt,
 *               including timestamp, student info, course info, and result.
 */
typedef struct {
    char timestamp[30];   /* HH:MM:SS.mmm format */
    int  student_id;
    char student_name[50];
    int  priority;
    int  course_id;
    char course_name[COURSE_NAME_LEN];
    int  result;          /* 1 = Success, 0 = Failed */
    char reason[100];     /* Reason for failure */
} RegistrationLog;

/* =============================================================================
 * GLOBAL STATE
 * =============================================================================
 */
Course          g_courses[MAX_COURSES];
int             g_num_courses     = 0;

StudentRequest  g_students[MAX_STUDENTS];
int             g_num_students    = 0;

RegistrationLog g_logs[MAX_STUDENTS * MAX_COURSES_PER_STU];
int             g_log_count       = 0;

/* Global statistics */
int             g_total_success   = 0;
int             g_total_failed    = 0;

/* Global synchronization primitives */
pthread_mutex_t g_log_mutex;           /* Protects log array */
pthread_mutex_t g_stats_mutex;         /* Protects global stats */
pthread_mutex_t g_print_mutex;         /* Protects console output */

/* Priority semaphore: high-priority students acquire this first */
sem_t           g_priority_sem;

/* Barrier to start all threads simultaneously */
pthread_barrier_t g_start_barrier;

/* =============================================================================
 * MODULE 6: LOGGING & OUTPUT — Helper Functions
 * =============================================================================
 */

/*
 * Function    : get_timestamp
 * Description : Generates a human-readable timestamp string in HH:MM:SS.mmm
 *               format using clock_gettime for millisecond precision.
 *               Used to timestamp every log entry.
 * Parameters  : buf (char*) - buffer to write timestamp into
 *               size (int)  - size of buffer
 */
void get_timestamp(char *buf, int size) {
    struct timespec ts;
    struct tm       tm_info;
    clock_gettime(CLOCK_REALTIME, &ts);
    localtime_r(&ts.tv_sec, &tm_info);
    int ms = (int)(ts.tv_nsec / 1000000);
    strftime(buf, size - 10, "%H:%M:%S", &tm_info);
    snprintf(buf + strlen(buf), 10, ".%03d", ms);
}

/*
 * Function    : log_registration
 * Description : Thread-safe function to record a registration attempt in the
 *               global log array. Uses g_log_mutex to protect the shared
 *               log buffer from concurrent write corruption.
 * Parameters  : student   - pointer to the student making the attempt
 *               course    - pointer to the course being registered for
 *               result    - 1 for success, 0 for failure
 *               reason    - reason string (for failures)
 */
void log_registration(StudentRequest *student, Course *course,
                      int result, const char *reason) {
    pthread_mutex_lock(&g_log_mutex);

    if (g_log_count < MAX_STUDENTS * MAX_COURSES_PER_STU) {
        RegistrationLog *log = &g_logs[g_log_count++];
        get_timestamp(log->timestamp, sizeof(log->timestamp));
        log->student_id = student->student_id;
        strncpy(log->student_name, student->student_name,
                sizeof(log->student_name) - 1);
        log->priority = student->priority;
        log->course_id = course->course_id;
        strncpy(log->course_name, course->course_name,
                sizeof(log->course_name) - 1);
        log->result = result;
        strncpy(log->reason, reason ? reason : "", sizeof(log->reason) - 1);
    }

    pthread_mutex_unlock(&g_log_mutex);
}

/*
 * Function    : print_colored
 * Description : Thread-safe colored terminal printing. Acquires g_print_mutex
 *               before writing to stdout to prevent interleaved output from
 *               concurrent threads garbling the display.
 * Parameters  : color  - ANSI color escape code string
 *               fmt    - printf-style format string
 *               ...    - variadic arguments
 */
void print_colored(const char *color, const char *fmt, ...) {
    va_list args;
    pthread_mutex_lock(&g_print_mutex);
    printf("%s", color);
    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);
    printf("%s", COLOR_RESET);
    fflush(stdout);
    pthread_mutex_unlock(&g_print_mutex);
}

/*
 * Function    : print_banner
 * Description : Displays the decorative ASCII banner for the registration
 *               system at program startup.
 */
void print_banner(void) {
    printf("\n");
    printf(COLOR_CYAN COLOR_BOLD);
    printf("╔══════════════════════════════════════════════════════════════════════════╗\n");
    printf("║          UNIVERSITY COURSE REGISTRATION SYSTEM  v1.0                    ║\n");
    printf("║          Concurrent Simulation Using POSIX Threads (pthreads)           ║\n");
    printf("╚══════════════════════════════════════════════════════════════════════════╝\n");
    printf(COLOR_RESET "\n");
}

/*
 * Function    : print_separator
 * Description : Prints a styled horizontal separator line for section breaks
 *               in the output to improve readability.
 * Parameters  : title - optional section title (NULL for plain separator)
 */
void print_separator(const char *title) {
    printf(COLOR_BLUE);
    if (title) {
        int len = (int)strlen(title);
        int pad = (76 - len - 2) / 2;
        printf("┌");
        for (int i = 0; i < pad; i++) printf("─");
        printf(" %s ", title);
        for (int i = 0; i < pad + (len % 2); i++) printf("─");
        printf("┐\n");
    } else {
        printf("├──────────────────────────────────────────────────────────────────────────┤\n");
    }
    printf(COLOR_RESET);
}

/* =============================================================================
 * MODULE 1: DATA INITIALIZATION
 * =============================================================================
 */

/*
 * Function    : init_courses
 * Description : Initializes the global course array with predefined course
 *               data including course IDs, names, and seat capacities.
 *               Initializes a per-course mutex for each course to enable
 *               fine-grained locking (allowing concurrent access to different
 *               courses at the same time).
 */
void init_courses(void) {
    /* Define 8 courses with varying seat capacities */
    struct { int id; char name[COURSE_NAME_LEN]; int seats; } course_data[] = {
        {101, "CS101 - Intro to Programming",         2},
        {102, "CS102 - Data Structures",              1},
        {103, "CS103 - Operating Systems",            3},
        {104, "CS104 - Computer Networks",            5},
        {105, "CS105 - Database Systems",             4},
        {106, "CS106 - Software Engineering",         3},
        {107, "CS107 - Artificial Intelligence",      2},
        {108, "CS108 - Computer Architecture",        6},
    };
    g_num_courses = (int)(sizeof(course_data) / sizeof(course_data[0]));

    for (int i = 0; i < g_num_courses; i++) {
        g_courses[i].course_id        = course_data[i].id;
        strncpy(g_courses[i].course_name, course_data[i].name,
                COURSE_NAME_LEN - 1);
        g_courses[i].total_seats      = course_data[i].seats;
        g_courses[i].available_seats  = course_data[i].seats;
        g_courses[i].registered_count = 0;

        /* Initialize per-course mutex — enables fine-grained concurrent access */
        if (pthread_mutex_init(&g_courses[i].seat_mutex, NULL) != 0) {
            fprintf(stderr, "ERROR: Failed to init mutex for course %d\n",
                    course_data[i].id);
            exit(EXIT_FAILURE);
        }
    }
}

/*
 * Function    : init_students
 * Description : Initializes student records for the simulation. Assigns
 *               student IDs, names, priority levels (high/low), and a
 *               randomized list of courses they want to register for.
 *               At least 30% of students are marked as high-priority
 *               (final-year students) to demonstrate priority handling.
 * Parameters  : num_students - total number of student threads to create
 */
void init_students(int num_students) {
    /* Predefined first names for variety in output */
    const char *names[] = {
        "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry",
        "Iris", "Jack", "Karen", "Liam", "Mia", "Noah", "Olivia", "Paul",
        "Quinn", "Rose", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xander",
        "Yara", "Zoe", "Aaron", "Beth", "Carl", "Donna"
    };
    int name_count = (int)(sizeof(names) / sizeof(names[0]));

    g_num_students = num_students;

    for (int i = 0; i < g_num_students; i++) {
        g_students[i].student_id     = 1000 + i;
        snprintf(g_students[i].student_name, 50, "%s_%d",
                 names[i % name_count], 1000 + i);

        /* Mark ~30% of students as high-priority (final-year) */
        g_students[i].priority = (i % 3 == 0) ? PRIORITY_HIGH : PRIORITY_LOW;

        /* Each student requests 1 to MAX_COURSES_PER_STU courses randomly */
        g_students[i].num_courses    = 1 + rand() % MAX_COURSES_PER_STU;
        g_students[i].success_count  = 0;
        g_students[i].fail_count     = 0;

        /* Randomly select unique courses for this student */
        int chosen[MAX_COURSES_PER_STU];
        int chosen_count = 0;

        for (int c = 0; c < g_students[i].num_courses; c++) {
            int course_idx;
            int duplicate;
            int attempts = 0;
            do {
                course_idx = rand() % g_num_courses;
                duplicate  = 0;
                for (int k = 0; k < chosen_count; k++) {
                    if (chosen[k] == course_idx) { duplicate = 1; break; }
                }
                attempts++;
            } while (duplicate && attempts < 20);

            if (!duplicate) {
                chosen[chosen_count++]              = course_idx;
                g_students[i].requested_courses[c]  = course_idx;
            } else {
                g_students[i].requested_courses[c]  = rand() % g_num_courses;
            }
        }
    }
}

/* =============================================================================
 * MODULE 3: SYNCHRONIZATION — find_course helper
 * =============================================================================
 */

/*
 * Function    : find_course
 * Description : Linear search to find a course by its array index.
 *               Returns a pointer to the matching course or NULL if not found.
 * Parameters  : courses    - pointer to the course array
 *               num_courses- number of courses in the array
 *               index      - array index to retrieve
 */
Course *find_course(Course *courses, int num_courses, int index) {
    if (index >= 0 && index < num_courses) {
        return &courses[index];
    }
    return NULL;
}

/* =============================================================================
 * MODULE 2 & 3: THREAD MANAGEMENT & SYNCHRONIZATION
 * =============================================================================
 */

/*
 * Function    : attempt_registration
 * Description : Core atomic registration function. Acquires the per-course
 *               mutex lock, checks seat availability, and either decrements
 *               the seat count (success) or records failure — all within
 *               a single critical section. This ensures:
 *               1. Seat count never goes negative (correctness)
 *               2. Check-then-act is atomic (no race condition)
 *               3. Only one thread modifies a course's seats at a time
 * 
 * Parameters  : student - pointer to the student requesting registration
 *               course  - pointer to the course being registered
 * Returns     : 1 if registered successfully, 0 if no seats available
 */
int attempt_registration(StudentRequest *student, Course *course) {
    int success = 0;

    /* ── CRITICAL SECTION BEGIN ── */
    pthread_mutex_lock(&course->seat_mutex);

    if (course->available_seats > 0) {
        /* Atomic: check AND decrement together under the lock */
        course->available_seats--;
        course->registered_count++;
        success = 1;
    }

    pthread_mutex_unlock(&course->seat_mutex);
    /* ── CRITICAL SECTION END ── */

    return success;
}

/*
 * Function    : student_thread_func
 * Description : Thread function executed by each student thread. Implements
 *               Module 4 (Priority Handling): high-priority students acquire
 *               g_priority_sem immediately while low-priority students wait
 *               briefly first, giving high-priority students a head start.
 *               
 *               Deadlock prevention (Module 5): Each student registers for
 *               one course at a time in a fixed order, preventing circular
 *               wait. Since each request acquires only ONE mutex at a time,
 *               there can be no hold-and-wait, eliminating deadlock entirely.
 *
 *               All threads synchronize at g_start_barrier to begin
 *               concurrently, maximizing contention for realistic testing.
 *
 * Parameters  : arg - pointer to ThreadArgs structure
 * Returns     : NULL (pthread convention)
 */
void *student_thread_func(void *arg) {
    ThreadArgs     *args    = (ThreadArgs *)arg;
    StudentRequest *student = args->student;
    Course         *courses = args->courses;
    int             n_courses = args->num_courses;

    /* ── MODULE 4: Priority Handling ──
     * All threads wait at the barrier to start concurrently.
     * After the barrier, high-priority students grab the priority semaphore
     * immediately. Low-priority students sleep a short random delay to yield
     * scheduling advantage to high-priority peers — simple but effective
     * starvation-avoidance: low-priority threads still run eventually.
     */
    pthread_barrier_wait(&g_start_barrier);

    if (student->priority == PRIORITY_HIGH) {
        /* High-priority: acquire semaphore immediately, no delay */
        sem_wait(&g_priority_sem);
    } else {
        /* Low-priority: small random delay (0–3 ms) before proceeding */
        struct timespec delay = {0, (rand() % 4) * 1000000L};
        nanosleep(&delay, NULL);
        sem_wait(&g_priority_sem);
    }

    /* ── MODULE 5: Deadlock Prevention ──
     * Register for courses ONE AT A TIME. Only one mutex is held at any
     * moment per thread. This breaks the "hold-and-wait" condition required
     * for deadlock. No circular dependency is ever possible.
     */
    for (int i = 0; i < student->num_courses; i++) {
        int     course_idx = student->requested_courses[i];
        Course *course     = find_course(courses, n_courses, course_idx);
        if (!course) continue;

        int success = attempt_registration(student, course);

        /* ── Thread-safe logging ── */
        if (success) {
            log_registration(student, course, 1, "");
            pthread_mutex_lock(&g_stats_mutex);
            g_total_success++;
            student->success_count++;
            pthread_mutex_unlock(&g_stats_mutex);

            print_colored(COLOR_GREEN,
                "  [%s] ✔ Student %4d %-18s [%s] → %-35s | Seats Left: %d\n",
                student->priority == PRIORITY_HIGH ? "HIGH" : "LOW ",
                student->student_id,
                student->student_name,
                student->priority == PRIORITY_HIGH ? "★ FINAL" : "REGULAR",
                course->course_name,
                course->available_seats);
        } else {
            log_registration(student, course, 0, "No Seats Available");
            pthread_mutex_lock(&g_stats_mutex);
            g_total_failed++;
            student->fail_count++;
            pthread_mutex_unlock(&g_stats_mutex);

            print_colored(COLOR_RED,
                "  [%s] ✘ Student %4d %-18s [%s] → %-35s | FULL\n",
                student->priority == PRIORITY_HIGH ? "HIGH" : "LOW ",
                student->student_id,
                student->student_name,
                student->priority == PRIORITY_HIGH ? "★ FINAL" : "REGULAR",
                course->course_name);
        }

        /* Small jitter between registrations to simulate real-world timing */
        struct timespec jitter = {0, (rand() % 3) * 500000L};
        nanosleep(&jitter, NULL);
    }

    /* Release priority semaphore — allows waiting threads to proceed */
    sem_post(&g_priority_sem);

    return NULL;
}

/* =============================================================================
 * MODULE 6: DISPLAY & REPORTING
 * =============================================================================
 */

/*
 * Function    : print_initial_state
 * Description : Displays the initial course configuration table before
 *               any registrations take place, showing course IDs, names,
 *               and initial seat counts in a formatted table.
 */
void print_initial_state(void) {
    print_separator("INITIAL COURSE CONFIGURATION");
    printf("\n");
    printf(COLOR_CYAN COLOR_BOLD);
    printf("  %-6s  %-40s  %10s\n", "ID", "Course Name", "Seats");
    printf("  ──────  ────────────────────────────────────────  ──────────\n");
    printf(COLOR_RESET);

    for (int i = 0; i < g_num_courses; i++) {
        printf("  %-6d  %-40s  %10d\n",
               g_courses[i].course_id,
               g_courses[i].course_name,
               g_courses[i].total_seats);
    }
    printf("\n");
}

/*
 * Function    : print_student_summary
 * Description : Displays a summary table of students participating in the
 *               simulation, showing their ID, name, priority, and the
 *               number of courses they requested.
 */
void print_student_summary(void) {
    print_separator("STUDENT THREAD OVERVIEW");
    printf("\n");
    printf(COLOR_CYAN COLOR_BOLD);
    printf("  %-8s  %-20s  %-10s  %-10s  %-10s\n",
           "ID", "Name", "Priority", "Requests", "Type");
    printf("  ────────  ────────────────────  ──────────  ──────────  ──────────\n");
    printf(COLOR_RESET);

    for (int i = 0; i < g_num_students; i++) {
        const char *prio_str  = (g_students[i].priority == PRIORITY_HIGH)
                                ? COLOR_YELLOW "★ HIGH  " COLOR_RESET
                                : "  LOW   ";
        const char *type_str  = (g_students[i].priority == PRIORITY_HIGH)
                                ? COLOR_YELLOW "Final-Year" COLOR_RESET
                                : "Regular";
        printf("  %-8d  %-20s  %s    %-10d  %s\n",
               g_students[i].student_id,
               g_students[i].student_name,
               prio_str,
               g_students[i].num_courses,
               type_str);
    }
    printf("\n");
}

/*
 * Function    : print_registration_log
 * Description : Prints the complete timestamped registration log after all
 *               threads have completed, showing every attempt with student
 *               info, course info, priority, and result. Successful entries
 *               are shown in green and failures in red.
 */
void print_registration_log(void) {
    print_separator("COMPLETE REGISTRATION LOG");
    printf("\n");
    printf(COLOR_CYAN COLOR_BOLD);
    printf("  %-13s  %-6s  %-18s  %-8s  %-35s  %-8s\n",
           "Timestamp", "St.ID", "Student", "Priority", "Course", "Result");
    printf("  ─────────────  ──────  ──────────────────  ────────  "
           "───────────────────────────────────  ────────\n");
    printf(COLOR_RESET);

    for (int i = 0; i < g_log_count; i++) {
        RegistrationLog *log = &g_logs[i];
        const char *result_color = log->result ? COLOR_GREEN : COLOR_RED;
        const char *result_str   = log->result ? "✔ PASS " : "✘ FAIL ";
        const char *prio_str     = (log->priority == PRIORITY_HIGH)
                                   ? COLOR_YELLOW "★ FINAL " COLOR_RESET
                                   : "REGULAR ";

        printf("  %s%-13s%s  %-6d  %-18s  %s  %-35s  %s%s%s\n",
               COLOR_WHITE, log->timestamp, COLOR_RESET,
               log->student_id,
               log->student_name,
               prio_str,
               log->course_name,
               result_color, result_str, COLOR_RESET);
    }
    printf("\n");
}

/*
 * Function    : print_final_seat_allocation
 * Description : Displays the final state of all courses after the simulation
 *               ends — showing registered count, remaining seats, and a
 *               visual seat-fill bar for each course.
 */
void print_final_seat_allocation(void) {
    print_separator("FINAL SEAT ALLOCATION");
    printf("\n");
    printf(COLOR_CYAN COLOR_BOLD);
    printf("  %-6s  %-38s  %8s  %9s  %10s\n",
           "ID", "Course Name", "Total", "Enrolled", "Remaining");
    printf("  ──────  ──────────────────────────────────────  "
           "────────  ─────────  ──────────\n");
    printf(COLOR_RESET);

    for (int i = 0; i < g_num_courses; i++) {
        Course *c = &g_courses[i];
        int     fill_blocks = (c->total_seats > 0)
                              ? (c->registered_count * 20 / c->total_seats)
                              : 0;

        /* Build visual fill bar */
        char bar[25];
        memset(bar, 0, sizeof(bar));
        for (int b = 0; b < 20; b++) {
            bar[b] = (b < fill_blocks) ? '#' : '-';
        }

        const char *status_color =
            (c->available_seats == 0) ? COLOR_RED :
            (c->available_seats <= 2) ? COLOR_YELLOW : COLOR_GREEN;

        printf("  %-6d  %-38s  %8d  %9d  %s%10d%s  [%s]\n",
               c->course_id,
               c->course_name,
               c->total_seats,
               c->registered_count,
               status_color,
               c->available_seats,
               COLOR_RESET,
               bar);
    }
    printf("\n");
}

/*
 * Function    : print_summary_statistics
 * Description : Prints the final summary statistics banner after simulation
 *               completes, including total successes, failures, and per-student
 *               breakdown. Validates correctness: checks that no course has
 *               negative seats (a correctness invariant).
 */
void print_summary_statistics(void) {
    print_separator("SUMMARY STATISTICS");
    printf("\n");

    int high_prio_success = 0, low_prio_success = 0;
    int high_prio_fail    = 0, low_prio_fail    = 0;

    for (int i = 0; i < g_num_students; i++) {
        if (g_students[i].priority == PRIORITY_HIGH) {
            high_prio_success += g_students[i].success_count;
            high_prio_fail    += g_students[i].fail_count;
        } else {
            low_prio_success  += g_students[i].success_count;
            low_prio_fail     += g_students[i].fail_count;
        }
    }

    printf(COLOR_BOLD "  ┌────────────────────────────────────────────┐\n");
    printf(           "  │           SIMULATION RESULTS               │\n");
    printf(           "  ├────────────────────────────────────────────┤\n");
    printf(COLOR_RESET);

    printf("  │  " COLOR_GREEN "%-28s : %6d" COLOR_RESET "         │\n",
           "Total Successful Registrations", g_total_success);
    printf("  │  " COLOR_RED   "%-28s : %6d" COLOR_RESET "         │\n",
           "Total Failed Registrations", g_total_failed);
    printf("  │  " COLOR_WHITE "%-28s : %6d" COLOR_RESET "         │\n",
           "Total Attempts", g_total_success + g_total_failed);
    printf("  │                                              │\n");
    printf("  │  " COLOR_YELLOW "%-28s : %6d" COLOR_RESET "         │\n",
           "★ High-Priority Successes", high_prio_success);
    printf("  │  " COLOR_YELLOW "%-28s : %6d" COLOR_RESET "         │\n",
           "★ High-Priority Failures", high_prio_fail);
    printf("  │  %-28s : %6d         │\n", "Low-Priority Successes",  low_prio_success);
    printf("  │  %-28s : %6d         │\n", "Low-Priority Failures",   low_prio_fail);
    printf("  │                                              │\n");
    printf("  │  %-28s : %6d         │\n", "Student Threads Created", g_num_students);
    printf("  │  %-28s : %6d         │\n", "Courses Available",       g_num_courses);

    /* ── Correctness Validation ── */
    int valid = 1;
    for (int i = 0; i < g_num_courses; i++) {
        if (g_courses[i].available_seats < 0) { valid = 0; break; }
    }
    printf("  │  " COLOR_GREEN "%-28s : %s" COLOR_RESET "         │\n",
           "Seat Invariant (≥ 0)", valid ? "  ✔ PASS" : "  ✘ FAIL");
    printf("  │  " COLOR_GREEN "%-28s : %s" COLOR_RESET "         │\n",
           "Deadlock Status", "NONE ✔ ");

    printf(COLOR_BOLD "  └────────────────────────────────────────────┘\n" COLOR_RESET);
    printf("\n");
}

/* =============================================================================
 * MAIN — Program Entry Point
 * =============================================================================
 */

/*
 * Function    : main
 * Description : Entry point. Initializes all data structures, global mutexes,
 *               semaphores, and barrier. Creates one POSIX thread per student
 *               request. Joins all threads to ensure clean termination. Then
 *               displays the full simulation report and cleans up all resources.
 *
 *               Two test modes are run:
 *               1. Mandatory Scenario (as per project spec): 10 students,
 *                  courses CS101(2), CS102(1), CS103(3)
 *               2. Stress Test: 50 concurrent student threads, 8 courses
 */
int main(void) {
    srand((unsigned int)time(NULL));
    print_banner();

    /* ────────────────────────────────────────────────────────────────────────
     * TEST SCENARIO 1: Mandatory Demonstration (per Section 13 of spec)
     * ────────────────────────────────────────────────────────────────────────
     * Courses : CS101 (2 seats), CS102 (1 seat), CS103 (3 seats)
     * Students: 10 threads, at least 3 high-priority
     */
    printf(COLOR_BOLD COLOR_MAGENTA);
    printf("══════════════════════════════════════════════════════════════════\n");
    printf("   TEST SCENARIO 1: MANDATORY DEMONSTRATION (Section 13)\n");
    printf("══════════════════════════════════════════════════════════════════\n");
    printf(COLOR_RESET "\n");

    /* Initialize mandatory scenario courses manually */
    g_num_courses = 3;

    struct { int id; const char *name; int seats; } mandatory[] = {
        {101, "CS101 - Intro to Programming", 2},
        {102, "CS102 - Data Structures",      1},
        {103, "CS103 - Operating Systems",    3},
    };
    for (int i = 0; i < 3; i++) {
        g_courses[i].course_id       = mandatory[i].id;
        strncpy(g_courses[i].course_name, mandatory[i].name, COURSE_NAME_LEN - 1);
        g_courses[i].total_seats     = mandatory[i].seats;
        g_courses[i].available_seats = mandatory[i].seats;
        g_courses[i].registered_count= 0;
        pthread_mutex_init(&g_courses[i].seat_mutex, NULL);
    }

    /* Initialize 10 students; first 3 are high-priority */
    g_num_students = 10;
    const char *s_names[] = {
        "Alice", "Bob", "Charlie", "Diana", "Eve",
        "Frank", "Grace", "Henry",  "Iris",  "Jack"
    };
    for (int i = 0; i < 10; i++) {
        g_students[i].student_id    = 2000 + i;
        snprintf(g_students[i].student_name, 50, "%s", s_names[i]);
        g_students[i].priority      = (i < 3) ? PRIORITY_HIGH : PRIORITY_LOW;
        g_students[i].num_courses   = 1 + rand() % 2;  /* 1 or 2 courses */
        g_students[i].success_count = 0;
        g_students[i].fail_count    = 0;
        for (int c = 0; c < g_students[i].num_courses; c++) {
            g_students[i].requested_courses[c] = rand() % 3;
        }
    }

    /* Initialize global synchronization primitives */
    pthread_mutex_init(&g_log_mutex,   NULL);
    pthread_mutex_init(&g_stats_mutex, NULL);
    pthread_mutex_init(&g_print_mutex, NULL);

    /* Priority semaphore: allow up to g_num_students concurrent holders
     * (effectively acts as a priority gate, not a strict limit) */
    sem_init(&g_priority_sem, 0, g_num_students);

    /* Barrier: all threads wait here before starting — maximizes contention */
    pthread_barrier_init(&g_start_barrier, NULL, g_num_students + 1);

    g_log_count    = 0;
    g_total_success = 0;
    g_total_failed  = 0;

    print_initial_state();
    print_student_summary();

    print_separator("LIVE REGISTRATION FEED");
    printf("\n");

    /* Create one thread per student */
    pthread_t   thread_handles[MAX_STUDENTS];
    ThreadArgs  thread_args[MAX_STUDENTS];

    for (int i = 0; i < g_num_students; i++) {
        thread_args[i].student    = &g_students[i];
        thread_args[i].courses    = g_courses;
        thread_args[i].num_courses = g_num_courses;

        if (pthread_create(&thread_handles[i], NULL,
                           student_thread_func, &thread_args[i]) != 0) {
            fprintf(stderr, "ERROR: Failed to create thread for student %d\n",
                    g_students[i].student_id);
        }
    }

    /* Release the barrier — all threads start concurrently */
    pthread_barrier_wait(&g_start_barrier);

    /* Join all threads — ensures clean termination before reporting */
    for (int i = 0; i < g_num_students; i++) {
        pthread_join(thread_handles[i], NULL);
    }

    printf("\n");
    print_registration_log();
    print_final_seat_allocation();
    print_summary_statistics();

    /* Cleanup Scenario 1 resources */
    for (int i = 0; i < g_num_courses; i++) {
        pthread_mutex_destroy(&g_courses[i].seat_mutex);
    }
    pthread_mutex_destroy(&g_log_mutex);
    pthread_mutex_destroy(&g_stats_mutex);
    pthread_mutex_destroy(&g_print_mutex);
    sem_destroy(&g_priority_sem);
    pthread_barrier_destroy(&g_start_barrier);

    /* ────────────────────────────────────────────────────────────────────────
     * TEST SCENARIO 2: STRESS TEST — 50 Students, 8 Courses
     * ────────────────────────────────────────────────────────────────────────
     */
    printf(COLOR_BOLD COLOR_MAGENTA);
    printf("══════════════════════════════════════════════════════════════════\n");
    printf("   TEST SCENARIO 2: STRESS TEST — 50 STUDENTS, 8 COURSES\n");
    printf("══════════════════════════════════════════════════════════════════\n");
    printf(COLOR_RESET "\n");

    /* Re-initialize everything for stress test */
    g_log_count     = 0;
    g_total_success = 0;
    g_total_failed  = 0;

    init_courses();
    init_students(50);

    pthread_mutex_init(&g_log_mutex,   NULL);
    pthread_mutex_init(&g_stats_mutex, NULL);
    pthread_mutex_init(&g_print_mutex, NULL);
    sem_init(&g_priority_sem, 0, g_num_students);
    pthread_barrier_init(&g_start_barrier, NULL, g_num_students + 1);

    print_initial_state();
    print_student_summary();

    print_separator("LIVE REGISTRATION FEED");
    printf("\n");

    pthread_t  s_handles[MAX_STUDENTS];
    ThreadArgs s_args[MAX_STUDENTS];

    for (int i = 0; i < g_num_students; i++) {
        s_args[i].student     = &g_students[i];
        s_args[i].courses     = g_courses;
        s_args[i].num_courses  = g_num_courses;

        if (pthread_create(&s_handles[i], NULL,
                           student_thread_func, &s_args[i]) != 0) {
            fprintf(stderr, "ERROR: Failed to create thread for student %d\n",
                    g_students[i].student_id);
        }
    }

    pthread_barrier_wait(&g_start_barrier);

    for (int i = 0; i < g_num_students; i++) {
        pthread_join(s_handles[i], NULL);
    }

    printf("\n");
    print_registration_log();
    print_final_seat_allocation();
    print_summary_statistics();

    /* Final cleanup */
    for (int i = 0; i < g_num_courses; i++) {
        pthread_mutex_destroy(&g_courses[i].seat_mutex);
    }
    pthread_mutex_destroy(&g_log_mutex);
    pthread_mutex_destroy(&g_stats_mutex);
    pthread_mutex_destroy(&g_print_mutex);
    sem_destroy(&g_priority_sem);
    pthread_barrier_destroy(&g_start_barrier);

    printf(COLOR_CYAN COLOR_BOLD);
    printf("╔══════════════════════════════════════════════════════════════════╗\n");
    printf("║   Simulation Complete. All threads terminated cleanly.  ✔        ║\n");
    printf("╚══════════════════════════════════════════════════════════════════╝\n");
    printf(COLOR_RESET "\n");

    return EXIT_SUCCESS;
}
