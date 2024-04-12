package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Collections;

public class LockUtil {

    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockType.equals(LockType.NL)) {
            return;
        }

        if (LockType.substitutable(lockContext.getEffectiveLockType(transaction), lockType)) {
            return;
        }

        acquireParentLocksIfNeeded(lockContext, transaction, lockType);
        acquireOrPromoteLock(lockContext, transaction, lockType);
    }

    private static void acquireParentLocksIfNeeded(LockContext lockContext, TransactionContext transaction, LockType lockType) {
        LockType requiredParentLockType = (lockType == LockType.S) ? LockType.IS : LockType.IX;
        ArrayList<LockContext> parentContexts = new ArrayList<>();
        LockContext parent = lockContext.parent;

        while (parent != null) {
            parentContexts.add(parent);
            parent = parent.parentContext();
        }

        Collections.reverse(parentContexts);
        for (LockContext parentIterator : parentContexts) {
            LockType currentParentLockType = parentIterator.getExplicitLockType(transaction);
            if (currentParentLockType.equals(LockType.NL)) {
                parentIterator.acquire(transaction, requiredParentLockType);
            } else if (!LockType.substitutable(currentParentLockType, requiredParentLockType)) {
                parentIterator.promote(transaction, requiredParentLockType);
            }
        }
    }


    private static void acquireOrPromoteLock(LockContext lockContext, TransactionContext transaction, LockType lockType) {
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        if (explicitLockType.equals(LockType.NL)) {
            lockContext.acquire(transaction, lockType);
            return;
        }

        if (lockType.equals(LockType.S)) {
            if (explicitLockType.equals(LockType.IS)) {
                lockContext.escalate(transaction);
            } else {
                lockContext.promote(transaction, LockType.SIX);
            }
        } else {
            if (!explicitLockType.equals(LockType.X)) {
                if (LockType.canBeParentLock(explicitLockType, lockType)) {
                    lockContext.promote(transaction, lockType);
                } else {
                    lockContext.escalate(transaction);
                    if (!lockContext.getExplicitLockType(transaction).equals(LockType.X)) {
                        lockContext.promote(transaction, LockType.X);
                    }
                }
            }
        }
    }
}
