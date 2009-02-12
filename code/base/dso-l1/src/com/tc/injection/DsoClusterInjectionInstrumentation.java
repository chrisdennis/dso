/*
 * All content copyright (c) 2003-2009 Terracotta, Inc., except as may otherwise be noted in a separate copyright notice.  All rights reserved.
 */
package com.tc.injection;

import com.tc.asm.ClassAdapter;
import com.tc.asm.ClassVisitor;
import com.tc.asm.MethodAdapter;
import com.tc.asm.MethodVisitor;
import com.tc.asm.Opcodes;
import com.tc.aspectwerkz.reflect.FieldInfo;
import com.tc.object.bytecode.ByteCodeUtil;
import com.tc.object.bytecode.ClassAdapterFactory;

public class DsoClusterInjectionInstrumentation implements InjectionInstrumentation {

  public ClassAdapterFactory getClassAdapterFactoryForFieldInjection(final FieldInfo fieldToInjectInto) {
    return new Factory(fieldToInjectInto);
  }

  private final static class Factory implements ClassAdapterFactory {
    private final FieldInfo fieldToInjectInto;

    private Factory(final FieldInfo fieldToInjectInto) {
      this.fieldToInjectInto = fieldToInjectInto;
    }

    public ClassAdapter create(final ClassVisitor visitor, final ClassLoader loader) {
      return new Adapter(visitor, loader);
    }

    private final class Adapter extends ClassAdapter implements Opcodes {

      private volatile boolean hasConstructor = false;

      private Adapter(final ClassVisitor cv, final ClassLoader caller) {
        super(cv);
      }

      @Override
      public MethodVisitor visitMethod(final int access, final String name, final String desc, final String signature, final String[] exceptions) {
        if ("<init>".equals(name)) {
          hasConstructor = true;
          return new DsoClusterConstructorInjection(super.visitMethod(access, name, desc, signature, exceptions));
        }

        return super.visitMethod(access, name, desc, signature, exceptions);
      }

      @Override
      public void visitEnd() {
        if (!hasConstructor) {
          hasConstructor = true;

          MethodVisitor mv = super.visitMethod(ACC_PUBLIC|ACC_SYNCHRONIZED, "<init>", "()V", null, null);
          mv.visitCode();
          addFieldInjectionCode(mv);
          mv.visitEnd();
        }

        super.visitEnd();
      }

      private void addFieldInjectionCode(final MethodVisitor mv) {
//        mv.visitVarInsn(ALOAD, 0);
//        mv.visitFieldInsn(GETFIELD, ByteCodeUtil.classNameToInternalName(fieldToInjectInto.getDeclaringType().getName()),
//                          fieldToInjectInto.getName(), 'L'+ByteCodeUtil.classNameToInternalName(fieldToInjectInto.getType().getName())+';');

//        Label labelFieldAlreadyInjected = new Label();
//        mv.visitJumpInsn(IFNULL, labelFieldAlreadyInjected);
        mv.visitVarInsn(ALOAD, 0);
        mv.visitMethodInsn(INVOKESTATIC, "com/tc/object/bytecode/ManagerUtil", "getManager", "()Lcom/tc/object/bytecode/Manager;");
        mv.visitMethodInsn(INVOKEINTERFACE, "com/tc/object/bytecode/Manager", "getDsoCluster", "()Lcom/tc/cluster/DsoCluster;");
        mv.visitFieldInsn(PUTFIELD, ByteCodeUtil.classNameToInternalName(fieldToInjectInto.getDeclaringType().getName()),
                          fieldToInjectInto.getName(), 'L'+ByteCodeUtil.classNameToInternalName(fieldToInjectInto.getType().getName())+';');
//        mv.visitLabel(labelFieldAlreadyInjected);
      }

      private final class DsoClusterConstructorInjection extends MethodAdapter {
        @Override
        public void visitCode() {
          addFieldInjectionCode(mv);

          super.visitCode();
        }

        public DsoClusterConstructorInjection(final MethodVisitor mv) {
          super(mv);
        }

      }
    }
  }
}