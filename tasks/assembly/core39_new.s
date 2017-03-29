.section .data
    value1: .float 323.4343
    value2: .float 324.5344
.section .text
    .globl main
main:
      movq $239864, %rcx
      s39:
        test  %ecx, %ecx
        andl %eax,%eax
        nop
        nop
        nop
        nop
        nop
        nop
        nop
        nop
        nop
        nop
        nop
        nop
        nop
        nop
        nop
	loop s39
