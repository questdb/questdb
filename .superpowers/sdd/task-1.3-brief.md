### Task 1.3: `SecurityContext.authorizeTableDelete` (OSS)

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/SecurityContext.java` (near `authorizeTableTruncate`, line 171)
- Modify: `core/src/main/java/io/questdb/cairo/security/AllowAllSecurityContext.java` (near line 228)
- Modify: `core/src/main/java/io/questdb/cairo/security/ReadOnlySecurityContext.java` (near line 263)
- Modify: **every other OSS `SecurityContext` implementor the compiler reports** (add the method following the AllowAll no-op / ReadOnly throw pattern).

**Interfaces:**
- Produces: `SecurityContext.authorizeTableDelete(TableToken tableToken)` — table-scoped (no column list), abstract.

- [ ] **Step 1: Add the abstract method.** In `SecurityContext.java`, directly after `void authorizeTableTruncate(TableToken tableToken);`:

```java
    void authorizeTableDelete(TableToken tableToken);
```

- [ ] **Step 2: Compile OSS to enumerate the implementors that now fail.**

Run: `mvn -q -pl core -am compile`
Expected: FAIL — one "does not override abstract method authorizeTableDelete" error per implementor. Record the list.

- [ ] **Step 3: Implement in `AllowAllSecurityContext` (no-op):**

```java
    @Override
    public void authorizeTableDelete(TableToken tableToken) {
    }
```

- [ ] **Step 4: Implement in `ReadOnlySecurityContext` (throw):**

```java
    @Override
    public void authorizeTableDelete(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }
```

- [ ] **Step 5: Implement in every other OSS implementor the compiler reported.** Use the no-op form for "allow-all"-style contexts and the throwing form for read-only/deny contexts. Re-run `mvn -q -pl core -am compile` until BUILD SUCCESS.

- [ ] **Step 6: Commit.**

```bash
git add core/src/main/java/io/questdb/cairo/SecurityContext.java core/src/main/java/io/questdb/cairo/security/
git commit -m "feat(delete): add SecurityContext.authorizeTableDelete (OSS impls)"
```

