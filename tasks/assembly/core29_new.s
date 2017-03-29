.section .data
    value1: .float 323.4343
    value2: .float 324.5344
.section .text
    .globl main
main:
       movq $114544, %rcx
       s29:
         subss  value1, %xmm0
         subss  %xmm1, %xmm0
         subss  %xmm1, %xmm0
         subss  %xmm1, %xmm0
         subss  %xmm1, %xmm0
         subss  %xmm1, %xmm0
         subss  %xmm1, %xmm0
	 loop s29
