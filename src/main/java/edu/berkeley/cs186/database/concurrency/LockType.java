package edu.berkeley.cs186.database.concurrency;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        if(a == LockType.NL){
            return true;
        } else if (a == LockType.IS) {
            return b != LockType.X;
        } else if (a == LockType.S) {
            return b == LockType.NL || b == LockType.IS || b == LockType.S;
        } else if (a == LockType.X) {
            return b == LockType.NL;
        } else if (a == LockType.IX) {
            return b != LockType.S && b != LockType.SIX && b != LockType.X;
        } else if (a == LockType.SIX) {
            return b == LockType.NL || b == LockType.IS;
        } else {
            throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (parentLockType == LockType.NL) {
            return childLockType == LockType.NL;
        } else if (parentLockType == LockType.IS) {
            return childLockType == LockType.NL || childLockType == LockType.IS || childLockType == LockType.S;
        } else if (parentLockType == LockType.IX) {
            return true;
        } else if (parentLockType == LockType.S) {
            return childLockType == LockType.NL;
        } else if (parentLockType == LockType.SIX) {
            return childLockType == LockType.NL || childLockType == LockType.X || childLockType == LockType.IX ;
        } else if (parentLockType == LockType.X) {
            return childLockType == LockType.NL;
        } else {
            throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (substitute == LockType.NL) {
            return required == LockType.NL;
        } else if (substitute == LockType.IS) {
            return required == LockType.NL || required == LockType.IS;
        } else if (substitute == LockType.IX) {
            return required == LockType.NL || required == LockType.IS || required == LockType.IX;
        } else if (substitute == LockType.S) {
            return required == LockType.NL || required == LockType.S;
        } else if (substitute == LockType.SIX) {
            return required != LockType.X && required != LockType.IX && required != LockType.IS;
        } else if (substitute == LockType.X) {
            return required != LockType.SIX && required != LockType.IX && required != LockType.IS;
        } else {
            throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

