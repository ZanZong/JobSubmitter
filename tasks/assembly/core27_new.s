.section .data
    value1: .float 323.4343
    value2: .float 324.5344
.section .text
    .globl main
main:
       movq $49322, %rcx
       s27:
         divss  value1, %xmm0
         divss  %xmm1, %xmm0
         divss  %xmm1, %xmm0
         divss  %xmm1, %xmm0
         divss  %xmm1, %xmm0
         divss  %xmm1, %xmm0
         divss  %xmm1, %xmm0
	 loop s27
