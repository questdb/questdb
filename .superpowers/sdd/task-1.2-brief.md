### Task 1.2: `isDeleteKeyword`

**Files:**
- Modify: `core/src/main/java/io/questdb/griffin/SqlKeywords.java` (near `isUpdateKeyword`, ~line 2457)
- Test: `core/src/test/java/io/questdb/test/griffin/SqlKeywordsTest.java` (create if absent; otherwise add a method)

**Interfaces:**
- Produces: `SqlKeywords.isDeleteKeyword(CharSequence): boolean`

- [ ] **Step 1: Write the failing test.** Create/extend `SqlKeywordsTest`:

```java
package io.questdb.test.griffin;

import io.questdb.griffin.SqlKeywords;
import org.junit.Assert;
import org.junit.Test;

public class SqlKeywordsTest {
    @Test
    public void testIsDeleteKeyword() {
        Assert.assertTrue(SqlKeywords.isDeleteKeyword("delete"));
        Assert.assertTrue(SqlKeywords.isDeleteKeyword("DELETE"));
        Assert.assertTrue(SqlKeywords.isDeleteKeyword("Delete"));
        Assert.assertFalse(SqlKeywords.isDeleteKeyword("delet"));
        Assert.assertFalse(SqlKeywords.isDeleteKeyword("deleted"));
        Assert.assertFalse(SqlKeywords.isDeleteKeyword("update"));
    }
}
```

- [ ] **Step 2: Run it, verify it fails to compile / fails.**

Run: `mvn -q -pl core test -Dtest=SqlKeywordsTest#testIsDeleteKeyword`
Expected: FAIL (`isDeleteKeyword` not defined).

- [ ] **Step 3: Implement.** In `SqlKeywords.java`, next to `isUpdateKeyword`:

```java
    public static boolean isDeleteKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'd'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'l'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 't'
                && (tok.charAt(5) | 32) == 'e';
    }
```

- [ ] **Step 4: Run, verify pass.**

Run: `mvn -q -pl core test -Dtest=SqlKeywordsTest#testIsDeleteKeyword`
Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add core/src/main/java/io/questdb/griffin/SqlKeywords.java core/src/test/java/io/questdb/test/griffin/SqlKeywordsTest.java
git commit -m "feat(delete): add isDeleteKeyword recognizer"
```

