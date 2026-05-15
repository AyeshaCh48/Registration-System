# 🎓 University Course Registration System

## 📌 Overview
The University Course Registration System is a high-performance concurrent simulation written in C that models real-world course enrollment. It uses multi-threading and synchronization techniques to manage shared course seats while ensuring fairness, consistency, and thread safety under high contention.

---

## 🚀 Features
- Each student modeled as an independent POSIX thread (`pthread`)  
- Mutex locks to protect critical sections and shared resources  
- Semaphores for controlled and priority-based access  
- Priority scheduling for final-year students with starvation avoidance  
- Thread barrier synchronization for simultaneous execution start  
- Automated logging of registrations and final statistics  

---

## 🛠️ Technologies Used
- C Language  
- POSIX Threads (pthreads)  
- Linux Systems Programming  
- Mutexes, Semaphores, Barriers  
- Linked lists and arrays for thread-safe logging  

---

## ▶️ How to Run

### 1. Compile
```bash
gcc -o registration registration.c -lpthread 
