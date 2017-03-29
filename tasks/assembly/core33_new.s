.section .data
    value1: .float 323.4343
    value2: .float 324.5344
.section .text
    .globl main
main:
      movq $344054, %rcx
      s33:
        divss  value1, %xmm0
	loop s33
