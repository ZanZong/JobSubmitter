.section .data
    value1: .float 323.4343
    value2: .float 324.5344
.section .text
    .globl main
main:
      movq $267412, %rcx
      s35:
        cld
        mulss  value2, %xmm1
	loop s35
